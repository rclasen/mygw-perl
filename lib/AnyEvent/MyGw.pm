#
# Copyright (c) 2015 Rainer Clasen
# 
# This program is free software; you can redistribute it and/or modify
# it under the terms described in the file LICENSE included in this
# distribution.
#

=pod

=head1 NAME

AnyEvent::MyGw - talk to MySensors Gateways

=head1 SYNOPSIS

 use AnyEvent::MyGw;

 my $mygw = AnyEvent::MyGw->new(
	host	=> $addr,
	port	=> $port,
	on_<whaterver> => sub { print "got <whatever> callback\n"; },
 );

 # then, within your event loop:
 TODO

=head1 DESCRIPTION

provide an abstract interface to the MySensors Serial protocol (which is also
used by Ethernet Gateways).

For using an actual Serial port, use Socat:

 socat -ddddd TCP4-LISTEN:2001,fork,reuseaddr /dev/ttyUSB0,raw,echo=0,b115200

This is the base to build your own MySensors Controller.

=head1 CONSTRUCTOR

=over 4

=item new()

=over 4

=item host	=> $address_or_name

=item port	=> $port

=item reconnect	=> $reconnect_interval

=item timeout	=> $connect_timeout

=back

=back

=head1 METHODS

=over 4

=item todo( )

send a todo command to dudl and provide result to callback. 

=back

=head1 CALLBACKS

=over 4

=item on_connected

=item on_disconnect

=item on_error

=back

=cut
package AnyEvent::MyGw;

use warnings;
use strict;

use Carp;
use AnyEvent;
use AnyEvent::Handle;
use AnyEvent::Socket;
use Sub::Name;

our $VERSION = 0.01;

our(
	%misc,
	%cmds,
	%sensortypes,
	%valtypes,
	%inttypes,
	%sttypes,
);

our( %sensornames, %valnames );

# constants from MyMessage.h:
BEGIN {
	%misc = (
		# special node IDs
		NID_GW		=> 0,
		NID_BCAST	=> 255,

		# special child IDs
		CID_NODE	=> 255,
		CID_CONTROL	=> 200,
		CID_BATTERY	=> 201,

		# payload maximum size
		PAYLOAD_MAX	=> 25,
		# firmware data block size:
		FWDATA_MAX	=> 16,
	);

	%cmds = (
		C_PRES	=> 0,
		C_SET	=> 1,
		C_REQ	=> 2,
		C_INT	=> 3,
		C_STR	=> 4,
	);

	# types for C_PRES:
	%sensortypes = (
		S_DOOR					=> 0,	# !< Door sensor, V_TRIPPED, V_ARMED
		S_MOTION				=> 1,	# !< Motion sensor, V_TRIPPED, V_ARMED
		S_SMOKE					=> 2,	# !< Smoke sensor, V_TRIPPED, V_ARMED
		S_BINARY				=> 3,	# !< Binary light or relay, V_STATUS, V_WATT
		S_LIGHT					=> 3,	# !< \deprecated Same as S_BINARY, **** DEPRECATED, DO NOT USE ****
		S_DIMMER				=> 4,	# !< Dimmable light or fan device, V_STATUS (on/off), V_PERCENTAGE (dimmer level 0-100), V_WATT
		S_COVER					=> 5,	# !< Blinds or window cover, V_UP, V_DOWN, V_STOP, V_PERCENTAGE (open/close to a percentage)
		S_TEMP					=> 6,	# !< Temperature sensor, V_TEMP
		S_HUM					=> 7,	# !< Humidity sensor, V_HUM
		S_BARO					=> 8,	# !< Barometer sensor, V_PRESSURE, V_FORECAST
		S_WIND					=> 9,	# !< Wind sensor, V_WIND, V_GUST
		S_RAIN					=> 10,	# !< Rain sensor, V_RAIN, V_RAINRATE
		S_UV					=> 11,	# !< Uv sensor, V_UV
		S_WEIGHT				=> 12,	# !< Personal scale sensor, V_WEIGHT, V_IMPEDANCE
		S_POWER					=> 13,	# !< Power meter, V_WATT, V_KWH, V_VAR, V_VA, V_POWER_FACTOR
		S_HEATER				=> 14,	# !< Header device, V_HVAC_SETPOINT_HEAT, V_HVAC_FLOW_STATE, V_TEMP
		S_DISTANCE				=> 15,	# !< Distance sensor, V_DISTANCE
		S_LIGHT_LEVEL			=> 16,	# !< Light level sensor, V_LIGHT_LEVEL (uncalibrated in percentage),  V_LEVEL (light level in lux)
		S_ARDUINO_NODE			=> 17,	# !< Used (internally) for presenting a non-repeating Arduino node
		S_ARDUINO_REPEATER_NODE	=> 18,	# !< Used (internally) for presenting a repeating Arduino node
		S_LOCK					=> 19,	# !< Lock device, V_LOCK_STATUS
		S_IR					=> 20,	# !< IR device, V_IR_SEND, V_IR_RECEIVE
		S_WATER					=> 21,	# !< Water meter, V_FLOW, V_VOLUME
		S_AIR_QUALITY			=> 22,	# !< Air quality sensor, V_LEVEL
		S_CUSTOM				=> 23,	# !< Custom sensor
		S_DUST					=> 24,	# !< Dust sensor, V_LEVEL
		S_SCENE_CONTROLLER		=> 25,	# !< Scene controller device, V_SCENE_ON, V_SCENE_OFF.
		S_RGB_LIGHT				=> 26,	# !< RGB light. Send color component data using V_RGB. Also supports V_WATT
		S_RGBW_LIGHT			=> 27,	# !< RGB light with an additional White component. Send data using V_RGBW. Also supports V_WATT
		S_COLOR_SENSOR			=> 28,	# !< Color sensor, send color information using V_RGB
		S_HVAC					=> 29,	# !< Thermostat/HVAC device. V_HVAC_SETPOINT_HEAT, V_HVAC_SETPOINT_COLD, V_HVAC_FLOW_STATE, V_HVAC_FLOW_MODE, V_TEMP
		S_MULTIMETER			=> 30,	# !< Multimeter device, V_VOLTAGE, V_CURRENT, V_IMPEDANCE
		S_SPRINKLER				=> 31,	# !< Sprinkler, V_STATUS (turn on/off), V_TRIPPED (if fire detecting device)
		S_WATER_LEAK			=> 32,	# !< Water leak sensor, V_TRIPPED, V_ARMED
		S_SOUND					=> 33,	# !< Sound sensor, V_TRIPPED, V_ARMED, V_LEVEL (sound level in dB)
		S_VIBRATION				=> 34,	# !< Vibration sensor, V_TRIPPED, V_ARMED, V_LEVEL (vibration in Hz)
		S_MOISTURE				=> 35,	# !< Moisture sensor, V_TRIPPED, V_ARMED, V_LEVEL (water content or moisture in percentage?)
		S_INFO					=> 36,	# !< LCD text device / Simple information device on controller, V_TEXT
		S_GAS					=> 37,	# !< Gas meter, V_FLOW, V_VOLUME
		S_GPS					=> 38,	# !< GPS Sensor, V_POSITION
		S_WATER_QUALITY			=> 39	# !< V_TEMP, V_PH, V_ORP, V_EC, V_STATUS
	);
	%sensornames = map { $sensortypes{$_} => $_ } keys %sensortypes;

	# types for C_SET / C_REQ
	%valtypes = (
		V_TEMP					=> 0,	# !< S_TEMP. Temperature S_TEMP, S_HEATER, S_HVAC
		V_HUM					=> 1,	# !< S_HUM. Humidity
		V_STATUS				=> 2,	# !< S_BINARY, S_DIMMER, S_SPRINKLER, S_HVAC, S_HEATER. Used for setting/reporting binary (on/off) status. 1=on, 0=off
		V_LIGHT					=> 2,	# !< \deprecated Same as V_STATUS, **** DEPRECATED, DO NOT USE ****
		V_PERCENTAGE			=> 3,	# !< S_DIMMER. Used for sending a percentage value 0-100 (%).
		V_DIMMER				=> 3,	# !< \deprecated Same as V_PERCENTAGE, **** DEPRECATED, DO NOT USE ****
		V_PRESSURE				=> 4,	# !< S_BARO. Atmospheric Pressure
		V_FORECAST				=> 5,	# !< S_BARO. Whether forecast. string of "stable", "sunny", "cloudy", "unstable", "thunderstorm" or "unknown"
		V_RAIN					=> 6,	# !< S_RAIN. Amount of rain
		V_RAINRATE				=> 7,	# !< S_RAIN. Rate of rain
		V_WIND					=> 8,	# !< S_WIND. Wind speed
		V_GUST					=> 9,	# !< S_WIND. Gust
		V_DIRECTION				=> 10,	# !< S_WIND. Wind direction 0-360 (degrees)
		V_UV					=> 11,	# !< S_UV. UV light level
		V_WEIGHT				=> 12,	# !< S_WEIGHT. Weight(for scales etc)
		V_DISTANCE				=> 13,	# !< S_DISTANCE. Distance
		V_IMPEDANCE				=> 14,	# !< S_MULTIMETER, S_WEIGHT. Impedance value
		V_ARMED					=> 15,	# !< S_DOOR, S_MOTION, S_SMOKE, S_SPRINKLER. Armed status of a security sensor. 1 => Armed, 0 => Bypassed
		V_TRIPPED				=> 16,	# !< S_DOOR, S_MOTION, S_SMOKE, S_SPRINKLER, S_WATER_LEAK, S_SOUND, S_VIBRATION, S_MOISTURE. Tripped status of a security sensor. 1 => Tripped, 0
		V_WATT					=> 17,	# !< S_POWER, S_BINARY, S_DIMMER, S_RGB_LIGHT, S_RGBW_LIGHT. Watt value for power meters
		V_KWH					=> 18,	# !< S_POWER. Accumulated number of KWH for a power meter
		V_SCENE_ON				=> 19,	# !< S_SCENE_CONTROLLER. Turn on a scene
		V_SCENE_OFF				=> 20,	# !< S_SCENE_CONTROLLER. Turn of a scene
		V_HVAC_FLOW_STATE		=> 21,	# !< S_HEATER, S_HVAC. HVAC flow state ("Off", "HeatOn", "CoolOn", or "AutoChangeOver")
		V_HEATER				=> 21,	# !< \deprecated Same as V_HVAC_FLOW_STATE, **** DEPRECATED, DO NOT USE ****
		V_HVAC_SPEED			=> 22,	# !< S_HVAC, S_HEATER. HVAC/Heater fan speed ("Min", "Normal", "Max", "Auto")
		V_LIGHT_LEVEL			=> 23,	# !< S_LIGHT_LEVEL. Uncalibrated light level. 0-100%. Use V_LEVEL for light level in lux
		V_VAR1					=> 24,	# !< VAR1
		V_VAR2					=> 25,	# !< VAR2
		V_VAR3					=> 26,	# !< VAR3
		V_VAR4					=> 27,	# !< VAR4
		V_VAR5					=> 28,	# !< VAR5
		V_UP					=> 29,	# !< S_COVER. Window covering. Up
		V_DOWN					=> 30,	# !< S_COVER. Window covering. Down
		V_STOP					=> 31,	# !< S_COVER. Window covering. Stop
		V_IR_SEND				=> 32,	# !< S_IR. Send out an IR-command
		V_IR_RECEIVE			=> 33,	# !< S_IR. This message contains a received IR-command
		V_FLOW					=> 34,	# !< S_WATER. Flow of water (in meter)
		V_VOLUME				=> 35,	# !< S_WATER. Water volume
		V_LOCK_STATUS			=> 36,	# !< S_LOCK. Set or get lock status. 1=Locked, 0=Unlocked
		V_LEVEL					=> 37,	# !< S_DUST, S_AIR_QUALITY, S_SOUND (dB), S_VIBRATION (hz), S_LIGHT_LEVEL (lux)
		V_VOLTAGE				=> 38,	# !< S_MULTIMETER
		V_CURRENT				=> 39,	# !< S_MULTIMETER
		V_RGB					=> 40,	# !< S_RGB_LIGHT, S_COLOR_SENSOR. Sent as ASCII hex: RRGGBB (RR=red, GG=green, BB=blue component)
		V_RGBW					=> 41,	# !< S_RGBW_LIGHT. Sent as ASCII hex: RRGGBBWW (WW=white component)
		V_ID					=> 42,	# !< Used for sending in sensors hardware ids (i.e. OneWire DS1820b).
		V_UNIT_PREFIX			=> 43,	# !< Allows sensors to send in a string representing the unit prefix to be displayed in GUI, not parsed by controller! E.g. cm, m, km, inch.
		V_HVAC_SETPOINT_COOL	=> 44,	# !< S_HVAC. HVAC cool setpoint (Integer between 0-100)
		V_HVAC_SETPOINT_HEAT	=> 45,	# !< S_HEATER, S_HVAC. HVAC/Heater setpoint (Integer between 0-100)
		V_HVAC_FLOW_MODE		=> 46,	# !< S_HVAC. Flow mode for HVAC ("Auto", "ContinuousOn", "PeriodicOn")
		V_TEXT					=> 47,	# !< S_INFO. Text message to display on LCD or controller device
		V_CUSTOM				=> 48,	# !< Custom messages used for controller/inter node specific commands, preferably using S_CUSTOM device type.
		V_POSITION				=> 49,	# !< GPS position and altitude. Payload: latitude;longitude;altitude(m). E.g. "55.722526;13.017972;18"
		V_IR_RECORD				=> 50,	# !< Record IR codes S_IR for playback
		V_PH					=> 51,	# !< S_WATER_QUALITY, water PH
		V_ORP					=> 52,	# !< S_WATER_QUALITY, water ORP : redox potential in mV
		V_EC					=> 53,	# !< S_WATER_QUALITY, water electric conductivity μS/cm (microSiemens/cm)
		V_VAR					=> 54,	# !< S_POWER, Reactive power: volt-ampere reactive (var)
		V_VA					=> 55,	# !< S_POWER, Apparent power: volt-ampere (VA)
		V_POWER_FACTOR			=> 56,	# !< S_POWER, Ratio of real power to apparent power: floating point value in the range [-1,..,1]
	);
	%valnames = map { $valtypes{$_} => $_ } keys %valtypes;

	# types for C_INT:
	%inttypes = (
		I_BATTERY_LEVEL			=> 0,
		I_TIME				=> 1,
		I_VERSION			=> 2,
		I_ID_REQUEST			=> 3,
		I_ID_RESPONSE			=> 4,
		I_INCLUSION_MODE		=> 5,
		I_CONFIG			=> 6,
		I_FIND_PARENT			=> 7,
		I_FIND_PARENT_RESPONSE		=> 8,
		I_LOG_MESSAGE			=> 9,
		I_CHILDREN			=> 10,
		I_SKETCH_NAME			=> 11,
		I_SKETCH_VERSION		=> 12,
		I_REBOOT			=> 13,
		I_GATEWAY_READY			=> 14,
		I_SIGNING_PRESENTATION		=> 15,
		I_NONCE_REQUEST			=> 16,
		I_NONCE_RESPONSE		=> 17,
		I_HEARTBEAT_REQUEST		=> 18,
		I_PRESENTATION			=> 19,
		I_DISCOVER_REQUEST		=> 20,
		I_DISCOVER_RESPONSE		=> 21,
		I_HEARTBEAT_RESPONSE		=> 22,
		I_LOCKED			=> 23,
		I_PING				=> 24,
		I_PONG				=> 25,
		I_REGISTRATION_REQUEST		=> 26,
		I_REGISTRATION_RESPONSE		=> 27,
		I_DEBUG				=> 28,
		I_SIGNAL_REPORT_REQUEST		=> 29,
		I_SIGNAL_REPORT_REVERSE		=> 30,
		I_SIGNAL_REPORT_RESPONSE	=> 31,
		I_PRE_SLEEP_NOTIFICATION	=> 32,
		I_POST_SLEEP_NOTIFICATION	=> 33,
	);

	# types for C_STR:
	%sttypes = (
		ST_FIRMWARE_CONFIG_REQUEST	=> 0,
		ST_FIRMWARE_CONFIG_RESPONSE	=> 1,
		ST_FIRMWARE_REQUEST		=> 2,
		ST_FIRMWARE_RESPONSE		=> 3,
		ST_SOUND			=> 4,
		ST_IMAGE			=> 5,
	);
}

use constant \%misc;
use constant \%cmds;
use constant \%sensortypes;
use constant \%valtypes;
use constant \%inttypes;
use constant \%sttypes;

use base 'Exporter';

our @EXPORT_OK		= (
	keys(%misc),
	keys(%cmds),
	keys(%sensortypes),
	keys(%valtypes),
	keys(%inttypes),
	keys(%sttypes),
);
our %EXPORT_TAGS	= (
	constants	=> \@EXPORT_OK,
	misc		=> [ keys(%misc) ],
	cmds		=> [ keys(%cmds) ],
	sensortypes	=> [ keys(%sensortypes) ],
	valtypes	=> [ keys(%valtypes) ],
	inttypes	=> [ keys(%inttypes) ],
	sttypes		=> [ keys(%sttypes) ],
);

# inlined: sub DEBUG(){1 or 0} based on ENV:
BEGIN {
	no strict 'refs';
	*DEBUG = $ENV{ANYEVENT_MYGW_DEBUG} ? sub(){1} : sub(){0};
}
sub DPRINT { DEBUG && print STDERR time.' '.$_[0]."\n"; }

sub new {
	my( $proto, %a ) = @_;

	my $self = bless {
		# connection settings
		host		=> $a{host},
		port		=> $a{port}||5003,
		reconnect	=> $a{reconnect}||10,
		timeout		=> $a{timeout}||30,
		# connection callbacks
		on_gwlog	=> $a{on_gwlog},
		on_connected	=> $a{on_connected},
		on_disconnect	=> $a{on_disconnect},
		on_error	=> $a{on_error},
		on_gwlog	=> $a{on_gwlog},

		# msg settings
		acktimeout	=> 3,
		maxqueue	=> 25,
		# msg callbacks
		on_idrequest	=> $a{on_idrequest},
		on_present	=> $a{on_present},
		on_battery	=> $a{on_battery},
		on_sketch_name	=> $a{on_sketch_name},
		on_sketch_version	=> $a{on_sketch_version},
		on_lib_version	=> $a{on_lib_version},
		on_set		=> $a{on_set},
		on_req		=> $a{on_req},
		on_stream	=> $a{on_stream},
		on_fwstart	=> $a{on_fwstart},

		# internal:
		sock	=> undef,
		rwatch	=> undef,
		node	=> {
			# $nodeid => {
				# noack => [ { # \%req:
					# msg => $msg,
					# cb => $sub, // optional
					# timeout => seconds,
				# },
				# wakeup => [ $req, ..],
				# direct => [ $req, ..],
				# pending => $req,
			# },
		},
	}, ref $proto || $proto;

	$self->connect;

	return $self;
}

sub DESTROY {
	my( $self ) = @_;

	DEBUG && DPRINT "destroy $self";
	$self->{rwatch} = undef;
	$self->disconnect('destroy');
	return;
}

sub _new_node {
	return {
		noack	=> [],
		direct	=> [],
		wakeup	=> [],
		pending	=> undef,
		seen	=> 0,
	};
}

sub nodeDelete {
	my( $self, $id ) = @_;

	my $node = delete $self->{node}{$id}
		or return;

	my $p = $node->{pending}
		or return;

	delete $p->{watch};

	foreach my $send ( $p, @{$node->{noack}}, @{$node->{wakeup}}, @{$node->{direct}} ){
		$send or next;

		$send->{cb} && $send->{cb}->( $self, undef );
	}

	return;
}

sub nodeSeen {
	my( $self, $id ) = @_;

	my $node = $self->{node}{$id}
		or return;

	return $node->{seen};
}

sub disconnect {
	my( $self, $msg ) = @_;

	DEBUG && DPRINT "disconnect: $msg";
	$self->{rwatch} = undef;

	if( $self->{sock} ){
		$self->{on_error} && $self->{on_error}->($msg, $self) if $msg;
		$self->{on_disconnect} && $self->{on_disconnect}->($self);
		$self->{sock}->destroy;
	}

	$self->{sock} = undef;

	return;
}

sub connect {
	my( $self ) = @_;

	$self->disconnect('reconnect')
		if $self->{sock};

	$self->{rwatch} = undef;

	Scalar::Util::weaken( my $weak = $self );

	$self->{sock} = AnyEvent::Handle->new(
		connect		=> [$self->{host}, $self->{port}],
		timeout		=> $self->{timeout},
		keepalive	=> 1,
		on_connect	=> subname( on_connect => sub {
			my( $h, $host, $port, $retry ) = @_;

			$weak or return;

			$weak->{on_connected} && $weak->{on_connected}->($weak, $host, $port);

			$h->push_read( line => subname( read_line => sub {
				$weak or return;
				$weak->_read( $_[1] );
			}) );
		}),
		on_timeout	=> subname( on_timeout => sub {
			$weak or return;
			$weak->{active}
				or return;
			$weak->disconnect( 'command timed out' );
			$weak->start_reconnect;
		}),
		on_error	=> subname( on_error => sub {
			my( $h, $fatal, $msg ) = @_;
			$weak or return;
			$weak->disconnect( 'socket error: '. $msg );
			$weak->start_reconnect;
		}),
		on_eof		=> subname( on_eof => sub {
			$weak or return;
			$weak->disconnect( 'EOF' );
			$weak->start_reconnect;
		}),
	);

	return 1;
}

sub start_reconnect {
	my( $self ) = @_;

	$self->{reconnect}
		or return;

	Scalar::Util::weaken( my $weak = $self );

	$self->{rwatch} = AnyEvent->timer(
		after		=> $self->{reconnect},
		cb		=> subname( reconnect_timer => sub {
			$weak or return;
			$weak->connect;
		}),
	);

	return;
}

sub getsockname {
	my( $self ) = @_;

	$self->{sock}
		or return;

	my $fh = $self->{sock}->fh
		or return;

	my( $service, $local ) = AnyEvent::Socket::unpack_sockaddr( getsockname($fh) );

	return( $service, format_address($local) );
}

############################################################
# firmware store

sub fwGet {
	my( $self, $ftype, $fver ) = @_;

	my $t = $self->{fw}{$ftype}
		or return;
	my $fw = $t->{$fver}
		or return;

	$fw->{activity} = time;

	return $fw;
}

sub fwSet {
	my( $self, $ftype, $fver, $fwdata ) = @_;

	my $size = length($fwdata)
		or return;

	# TODO: pad on 128 bytes
	return; # TODO

	# TODO; $crc
	my $crc;

	$self->{fw}{$ftype}{$fver} = {
		data	=> $fwdata,
		blocks	=> $size / FWDATA_MAX,
		crc	=> $crc,
		type	=> $ftype,
		ver	=> $fver,
		activity	=> time,
	};
}

sub fwClear {
	my( $self, $ftype, $fver ) = @_;

	delete $self->{fw}{$ftype}{$fver};
}

sub fwList {
	my( $self ) = @_;

	my @res;

	foreach my $ftype ( keys %{$self->{fw}} ){
		foreach my $fver ( keys %{$self->{fw}{$ftype}} ){
			push @res, [ $ftype, $fver, $self->{fw}{$ftype}{$fver} ];
		}
	}

	return \@res;
}

############################################################
# protocol handling

# node-id ; child-sensor-id ; command ; ack ; type ; payload \n

# greeting
#0;255;3;0;14;Gateway startup complete.
#0;255;0;0;18;2.2.0-beta

# 102;0;2;0;46;HELLO

our $is_uint = qr/^\d+$/;
sub sendRaw {
	my( $self, $m ) = @_;

	$m and ref $m
		or return;

	$m->{node} =~ /$is_uint/ and $m->{node} <= 255
		or return; # TODO: die "bad node id";

	$m->{child} =~ /$is_uint/ and $m->{child} <= 255
		or return; # TODO:;die "bad child id";

	$m->{cmd} =~ /$is_uint/ and $m->{cmd} <= 4
		or return; # TODO: die "bad cmd";

	$m->{type} =~ /$is_uint/ and $m->{type} <= 255
		or return; # TODO: die "bad payload type";

	if( ! defined($m->{payload}) ){
		$m->{payload} = '';
	} elsif( length $m->{payload} > PAYLOAD_MAX ){
		return; # TODO: die "payload too big";
	}

	my $node = $self->{node}{$m->{node}} ||= _new_node;

	# TODO: ignore undef entries
	( @{$node->{noack}} + @{$node->{direct}} + @{$node->{wakeup}} < $self->{maxqueue} )
		or return; # TODO: die "queue full";

	my $send = {
		cb	=> $m->{cb},
		timeout	=> $m->{timeout},
		watch	=> undef,	# AE::timer set on send

		msg	=> sprintf("%d;%d;%d;%d;%d;%s",
			$m->{node},
			$m->{child},
			$m->{cmd},
			$m->{ack} ? 1 : 0,
			$m->{type},
			$m->{cmd} == 4
				? unpack('H*', $m->{payload})
				: $m->{payload}
		),
	};

	# add to proper queue:
	if( ! $m->{ack} ){
		my $q = $node->{noack};
		if( $m->{doqueue} ){
			push @$q, $send;
			Scalar::Util::weaken( $q->[-1] );
		} else {
			unshift @$q, $send;
		}

	} else {
		my $q = $m->{doqueue} ? $node->{wakeup} : $node->{direct};
		push @$q, $send;
		Scalar::Util::weaken( $q->[-1] );
	}

	$self->_flush( $m->{node}, ! $m->{doqueue} );
	return $send;
}

our $re_sep = qr/;/;
sub _read {
	my( $self, $line ) = @_;

	$self->{sock}
		or return;

	DEBUG && DPRINT "got line: $line";

	# decode
	my %m;
	@m{qw/ node child cmd ack type payload /} = split( $re_sep, $line, 6 ) or do {
		$self->disconnect( "bad data $line");
		$self->start_reconnect;
		return;
	};

	my $node = $self->{node}{$m{node}} ||= _new_node;
	$node->{seen} = time;

	# dispatch
	if( $m{ack} ){
		$self->_gotAck( \%m, $line );

	} elsif( $m{cmd} == C_SET ){
		$self->{on_set} && $self->{on_set}->($self, \%m );

	} elsif( $m{cmd} == C_REQ ){
		$self->{on_req} && $self->{on_req}->($self, \%m );

	} elsif( $m{cmd} == C_STR ){
		$m{payload} = pack('H*', $m{payload} )
			if defined $m{payload};

		if( $m{type} == ST_FIRMWARE_CONFIG_REQUEST ){
			$self->_replyFirmwareStart( \%m );

		} elsif( $m{type} == ST_FIRMWARE_REQUEST ){
			$self->_replyFirmwareBlock( \%m );

		} elsif( $m{type} == ST_FIRMWARE_CONFIG_RESPONSE
			|| $m{type} == ST_FIRMWARE_CONFIG_REQUEST ){
			# do nothing

		} else { # other stream
			$self->{on_stream} && $self->{on_stream}->($self, \%m );
		}

	} elsif( $m{cmd} == C_INT ){
		if( $m{type} == I_BATTERY_LEVEL ){
			$self->{on_battery} && $self->{on_battery}->($self, \%m );

		} elsif( $m{type} == I_TIME ){
			$self->sendTime({
				node	=> $m{node},
			});

		} elsif( $m{type} == I_CONFIG ){
			$self->_replyConfig( \%m );

		} elsif( $m{type} == I_SKETCH_NAME ){
			$self->{on_sketch_name} && $self->{on_sketch_name}->( $self, \%m );

		} elsif( $m{type} == I_SKETCH_VERSION ){
			$self->{on_sketch_version} && $self->{on_sketch_version}->( $self, \%m );

		} elsif( $m{type} == I_ID_REQUEST ){
			$self->_replyIdRequest( \%m );

		} elsif( $m{type} == I_VERSION ){
			$self->{on_lib_version} && $self->{on_lib_version}->( $self, \%m );

		} elsif( $m{type} == I_LOG_MESSAGE ){
			$self->{on_gwlog} && $self->{on_gwlog}->( $self, $m{payload} );

		# TODO: more internal commands

		} elsif( $m{type} == I_DEBUG
			|| $m{type} == I_HEARTBEAT_RESPONSE	# sent by myself
			|| $m{type} == I_ID_RESPONSE		# sent by myself
			|| $m{type} == I_REGISTRATION_RESPONSE	# sent by myself
			|| $m{type} == I_REGISTRATION_REQUEST	# done by gw itself
			){
			# do nothing

		} else {
			DEBUG && DPRINT "unhandled internal: $line";
		}

	} elsif( $m{cmd} == C_PRES ){
		$self->{on_present} && $self->{on_present}->($self, \%m );

	}

	$self->_flush( $m{node}, 1 );

	Scalar::Util::weaken( my $weak = $self );

	# wait... if there wasn't an error
	$self->{sock}->push_read( line => subname( read_line => sub {
		$weak or return;
		$weak->_read( $_[1] );
	}) ) if $self->{sock};

	return;
}

# we have:
# - ontraffic msg without ack
# - ontraffic msg with ack
# - instant msg without ack
# - instant msg with ack
# there can be only one ACK pending

# on ACK timeout:
# - send for next instant ACK

# on traffic / explicit send:
# - send instant without ack
# - and send ontraffic without ack
# - if no ack pending
#    - send instant ACK
#    - or send ontraffic ACK

# this needs 3 queues:
# - noack: head=instant, tail=ontraffic
# - direct
# - wakeup

sub _flush {
	my( $self, $id, $ontraffic ) = @_;

	my $sock = $self->{sock}
		or return;

	my $node = $self->{node}{$id}
		or return;

	if( $ontraffic ){
		while( @{$node->{noack}} ){
			my $send = shift @{$node->{noack}}
				or next;

			$sock->push_write( $send->{msg}."\n" );
			DEBUG && DPRINT "sent: $send->{msg}";
			$send->{cb} && $send->{cb}->( $self, {} );
		}
	}

	$node->{pending}
		and return;

	my $send;
	while( @{$node->{direct}} ){
		$send = shift @{$node->{direct}}
			and last;
	}
	if( $ontraffic && !$send){
		while( @{$node->{wakeup}} ){
			$send = shift @{$node->{wakeup}}
				and last;
		}
	}
	$send or return;

	$node->{pending} = $send,
	Scalar::Util::weaken( $node->{pending} );

	$sock->push_write( $send->{msg}."\n" );
	DEBUG && DPRINT "sent(ack): $send->{msg}";

	Scalar::Util::weaken( my $weak = $self );

	$send->{watch} = AnyEvent->timer(
		after	=> $send->{timeout} || $self->{acktimeout},
		cb	=> subname( ack_timeout => sub {
			$weak or return;

			my $node = $weak->{node}{$id};
			my $p = $node->{pending}
				or return;

			delete $p->{watch};
			$p->{cb} && $p->{cb}->( $weak, undef );
			delete $node->{pending};

			$self->_flush( $id );
		}),
	);

	return 1;
}

############################################################
# respond to traffic:

sub _gotAck {
	my( $self, $m, $raw ) = @_;

	my $node = $self->{node}{$m->{node}} or do {
		DEBUG && DPRINT "got ack from unknown src";
		return;
	};

	my $send = $node->{pending} or do {
		DEBUG && DPRINT "got spurious ack";
		return;
	};

	$raw eq $send->{msg} or do {
		DEBUG && DPRINT "got mismatching ack";
		return;
	};

	delete $send->{watch};
	DEBUG && DPRINT "got ack from node $m->{node}";

	$send->{cb} && $send->{cb}->( $self, $m );
	delete $node->{pending};

	return;
}

sub _replyIdRequest {
	my( $self, $m ) = @_;

	$self->{on_idrequest}
		or return; # TODO: die no idrequst

	$self->{on_idrequest}->( $self, $m )
		or return; # TODO: error no ID

	return 1;
}

sub _replyConfig {
	my( $self, $m ) = @_;

	$self->sendConfig({
		metric	=> 1,
	});
}

# TODO: test firmeware update
sub _replyFirmwareStart {
	my( $self, $m ) = @_;

	my( $ftype, $fver ) = unpack('vv', $m->{payload} )
		or return; # TODO: die bad fw payload

	$self->{on_fwstart} && $self->{on_fwstart}->( $self, $m->{id}, $ftype, $fver );
}

=pod

example on_fwstart callback:

 sub on_fwstart {
 	my( $self, $id, $ftype, $fver ) = @_;
 
 	# controller may override ftype/fver:
  
  	# TODO: check $self->fwGet, first
 
 	# event-driven loading of firmware:
 	weaken( my $weak = $self );
 	&loadFw(...., sub {
 		my $fwdata = shift;
  		$weak->fwSet( $ftype, $fver, $fwdata )
 			or return;
 
		$weak->sendFwUpdate({
			node => $id,
			ftype => $ftype,
			fver => $fver,
		});
	});
 }

=cut

sub _replyFirmwareBlock {
	my( $self, $m ) = @_;

	my( $ftype, $fver, $block ) = unpack('vvV', $m->{payload} )
		or return;

	my $fw = $self->fwGet( $ftype, $fver )
		or return;

	$self->sendRaw({
		node	=> $m->{node},
		child	=> CID_NODE,
		cmd	=> C_STR,
		type	=> ST_FIRMWARE_RESPONSE,
		payload => pack('vvv', $ftype, $fver, $block )
			. substr( $fw->{data}, $block * FWDATA_MAX, FWDATA_MAX ),
	});
}



############################################################
# mostly for internal use

sub sendId {
	my( $self, $a ) = @_;

	ref $a or return;
	$a->{newid} or return;

	$self->sendRaw({
		node	=> $a->{node} // NID_BCAST,
		child	=> CID_NODE,
		cmd	=> C_INT,
		type	=> I_TIME,
		payload	=> $a->{newid},
		doqueue	=> $a->{doqueue},
		cb	=> $a->{cb},
		ack	=> $a->{ack},
	});
}

sub sendConfig {
	my( $self, $a ) = @_;

	ref $a or return;
	defined $a->{node} or return;

	$self->sendRaw({
		node	=> $a->{node},
		child	=> CID_NODE,
		cmd	=> C_INT,
		type	=> I_CONFIG,
		payload	=> $a->{imperial} ? 'I' : 'M',
		doqueue	=> $a->{doqueue},
		cb	=> $a->{cb},
		ack	=> $a->{ack},
	});
}

sub sendFwUpdate {
	my( $self, $a ) = @_;

	ref $a or return;
	defined $a->{node} or return;

	my $fw = $self->fwGet( $a->{ftype}, $a->{fver} )
		or return;

	my $payload = pack( 'vvvv', $fw->{type}, $fw->{ver}, $fw->{blocks}, $fw->{crc} );

	$self->sendRaw({
		node	=> $a->{node},
		child	=> CID_NODE,
		cmd	=> C_STR,
		type	=> ST_FIRMWARE_CONFIG_RESPONSE,
		payload => $payload,
		doqueue => $a->{doqueue},
		cb	=> $a->{cb},
		ack	=> $a->{ack},
	});
}

############################################################
# user commands

sub sendTime {
	my( $self, $a ) = @_;

	$a ||= {};

	$self->sendRaw({
		node	=> $a->{node} // NID_BCAST,
		child	=> CID_NODE,
		cmd	=> C_INT,
		type	=> I_TIME,
		payload	=> time,
		doqueue => $a->{doqueue},
		cb	=> $a->{cb},
		ack	=> $a->{ack},
	});
}

sub sendBootRequest {
	my( $self, $a ) = @_;

	ref $a or return;
	defined $a->{node} or return;

	$self->sendRaw({
		node	=> $a->{node},
		child	=> CID_NODE,
		cmd	=> C_INT,
		type	=> I_REBOOT,
		doqueue => $a->{doqueue},
		cb	=> $a->{cb},
		ack	=> $a->{ack},
	});
}

sub sendPresentationRequest {
	my( $self, $a ) = @_;

	ref $a or return;
	defined $a->{node} or return;

	$self->sendRaw({
		node	=> $a->{node},
		child	=> CID_NODE,
		cmd	=> C_INT,
		type	=> I_PRESENTATION,
		doqueue => $a->{doqueue},
		cb	=> $a->{cb},
		ack	=> $a->{ack},
	});
}


# TODO: more commands:
# I_HEARTBEAT_REQUEST
# I_CHILDREN C - clear routes

1;

__END__

=head1 AUTHOR

Rainer Clasen

=head1 SEE ALSO

MySensors

=cut
