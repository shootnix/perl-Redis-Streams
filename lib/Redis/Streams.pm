package Redis::Streams;

use v5.14;
use strict;
use warnings;

use Carp qw/croak carp/;
use IO::Socket::INET;


sub new {
    my ($class, $args) = @_;

    my $self = bless {}, $class;
    return $self->_init($args);
}

sub _init {
    my ($self, $args) = @_;

    my $sock = $self->_connect($args->{host}, $args->{port});
    $self->{sock} = $sock;

    return $self;
}

sub add_message {
    my ($self, $stream, $msg_hash_ref, $options) = @_;

    croak "stream name required" unless defined $stream;
    croak "message must be a hash reference" unless ref($msg_hash_ref) eq 'HASH';
    croak "cannot add non-scalar value to Redis Stream" if grep { ref $_ } values %$msg_hash_ref;

    $options ||= {};
    my $id = $options->{id} // '*';

    my @args = ($stream, $id);
    while (my ($k, $v) = each %$msg_hash_ref) {
        push @args, $k, $v;
    }

    return $self->_send_command('XADD', @args);
}

sub read_messages {
    my ($self, $stream, $last_id) = @_;

    croak "stream required" unless defined $stream && !ref($stream);
    $last_id //= '0';
    croak "last_id must be scalar" if ref($last_id);

    my @args = ('STREAMS', $stream, $last_id);

    my $raw = $self->_send_command('XREAD', @args);

    return [] unless $raw && ref $raw eq 'ARRAY';

    my ($returned_stream, $entries) = @{$raw->[0]};
    croak "unexpected stream name in response" if $returned_stream ne $stream;

    return $self->_parse_entries($entries);
}

sub read_messages_blocking {
    my ($self, $stream, $last_id, $timeout_ms) = @_;

    croak "stream required" unless defined $stream && !ref($stream);

    $last_id //= '$';
    croak "last_id must be scalar" if ref($last_id);

    $timeout_ms //= 5000;
    croak "timeout_ms must be a positive integer" unless $timeout_ms =~ /^\d+$/ && $timeout_ms > 0;

    my @args = ('BLOCK', $timeout_ms, 'STREAMS', $stream, $last_id);

    my $raw = $self->_send_command('XREAD', @args);

    return [] unless $raw && ref $raw eq 'ARRAY';

    my ($returned_stream, $entries) = @{$raw->[0]};
    croak "unexpected stream name in response" if $returned_stream ne $stream;

    return $self->_parse_entries($entries);
}

sub read_messages_range {
    my ($self, $stream, $start, $end, $options) = @_;

    croak "stream required" unless defined $stream && !ref($stream);

    $start //= '-';
    $end   //= '+';

    croak "start and end must be scalars" if ref($start) || ref($end);
    croak "options must be hashref if provided" if defined($options) && ref($options) ne 'HASH';

    my @args = ($stream, $start, $end);

    if ($options && exists $options->{count}) {
        my $count = $options->{count};
        croak "count must be positive integer" unless defined($count) && $count =~ /^\d+$/ && $count > 0;
        unshift @args, ('COUNT', $count);
    }

    my $raw = $self->_send_command('XRANGE', @args);

    return $self->_parse_entries($raw);
}

sub delete_message {
    my ($self, $stream, $ids) = @_;

    croak "stream required" unless defined $stream && !ref($stream);
    croak "id required" unless defined $ids;

    my @id_list = ref($ids) eq 'ARRAY' ? @$ids : ($ids);

    return $self->_send_command('XDEL', $stream, @id_list);
}

sub delete_stream {
    my ($self, $stream) = @_;

    croak "stream required" unless defined $stream && !ref($stream);

    return $self->_send_command('DEL', $stream);
}

sub trim_stream {
    my ($self, $stream, $maxlen) = @_;

    croak "stream name required" unless defined $stream && !ref($stream);
    croak "maxlen must be a positive integer" unless defined($maxlen) && $maxlen =~ /^\d+$/ && $maxlen > 0;

    my @args = ($stream, 'MAXLEN', $maxlen);

    my $deleted = $self->_send_command('XTRIM', @args);

    return $deleted;
}

# --- Groups ---

sub create_group {
    my ($self, $stream, $group, $start_id) = @_;

    croak "stream name required" unless defined $stream && !ref($stream);
    croak "group name required" unless defined $group && !ref($group);

    $start_id //= '$';
    croak "start_id must be scalar" if ref($start_id);

    my @args = ($stream, $group, $start_id, 'MKSTREAM');

    my $resp = $self->_send_command('XGROUP', 'CREATE', @args);

    return $resp eq 'OK' ? 1 : 0;
}

sub read_messages_group {
    my ($self, $stream, $group, $consumer, $last_id, $options) = @_;

    croak "group name required" unless defined $group && !ref($group);
    croak "consumer name required" unless defined $consumer && !ref($consumer);
    croak "stream name required" unless defined $stream && !ref($stream);

    $last_id //= '>';

    croak "last_id must be scalar" if ref($last_id);

    $options ||= {};

    my @args = ('GROUP', $group, $consumer);

    push @args, ('COUNT', $options->{count}) if defined $options->{count};
    push @args, ('BLOCK', $options->{block}) if defined $options->{block};

    push @args, 'STREAMS', $stream, $last_id;

    my $raw = $self->_send_command('XREADGROUP', @args);

    use Data::Dumper;
    warn Dumper $raw;

    #my ($next_start_id, $entries) = @$raw;

    return $self->_parse_entries($raw->[0]);
}

sub ack_message {
    my ($self, $stream, $group, @ids) = @_;

    croak "stream name required" unless defined $stream && !ref($stream);
    croak "group name required" unless defined $group && !ref($group);
    croak "at least one id required" unless @ids;

    return $self->_send_command('XACK', $stream, $group, @ids);
}

sub pending_messages {
    my ($self, $stream, $group, $options) = @_;

    croak "stream name required" unless defined $stream && !ref($stream);
    croak "group name required" unless defined $group && !ref($group);

    $options ||= {};

    my $start = $options->{start} // '-';
    my $end   = $options->{end}   // '+';
    my $count = $options->{count} // 10;

    my @args = ($stream, $group, $start, $end, $count);

    push @args, $options->{consumer} if defined $options->{consumer};

    my $raw = $self->_send_command('XPENDING', @args);

    my @pending;
    foreach my $entry (@$raw) {
        my ($id, $consumer, $elapsed_ms, $delivery_count) = @$entry;
        push @pending, {
            id => $id,
            consumer => $consumer,
            idle => $elapsed_ms + 0,
            deliveries => $delivery_count + 0,
        };
    }

    return \@pending;
}

sub claim_message {
    my ($self, $stream, $group, $consumer, $ids_ref, $min_idle_time, $options) = @_;

    croak "stream name required" unless defined $stream && !ref($stream);
    croak "group name required" unless defined $group && !ref($group);
    croak "consumer name required" unless defined $consumer && !ref($consumer);
    croak "ids must be arrayref" unless ref($ids_ref) eq 'ARRAY' && @$ids_ref;
    croak "min_idle_time must be a number" unless defined $min_idle_time && $min_idle_time =~ /^\d+$/;

    $options ||= {};

    my @args = ($stream, $group, $consumer, $min_idle_time, @$ids_ref);

    push @args, 'JUSTID' if $options->{justid};
    push @args, ('IDLE', $options->{idle}) if defined $options->{idle};
    push @args, ('TIME', $options->{time}) if defined $options->{time};
    push @args, ('RETRYCOUNT', $options->{retrycount}) if defined $options->{retrycount};
    push @args, 'FORCE' if $options->{force};

    my $raw = $self->_send_command('XCLAIM', @args);

    if ($options->{justid}) {
        return $raw;  # просто список ID
    }

    return $self->_parse_entries($stream, $raw);
}

sub delete_consumer {
    my ($self, $stream, $group, $consumer) = @_;

    croak "stream name required" unless defined $stream && !ref($stream);
    croak "group name required" unless defined $group && !ref($group);
    croak "consumer name required" unless defined $consumer && !ref($consumer);

    return $self->_send_command('XGROUP', 'DELCONSUMER', $stream, $group, $consumer);
}

sub delete_group {
    my ($self, $stream, $group) = @_;

    croak "stream name required" unless defined $stream && !ref($stream);
    croak "group name required" unless defined $group && !ref($group);

    return $self->_send_command('XGROUP', 'DESTROY', $stream, $group);
}

sub set_id {
    my ($self, $stream, $group, $id) = @_;

    croak "stream name required" unless defined $stream && !ref($stream);
    croak "group name required" unless defined $group && !ref($group);
    croak "id required" unless defined $id && !ref($id);

    return $self->_send_command('XGROUP', 'SETID', $stream, $group, $id);
}

# --- Info ---

sub info_stream {
    my ($self, $stream) = @_;

    croak "stream name required" unless defined $stream && !ref($stream);

    my $raw = $self->_send_command('XINFO', 'STREAM', $stream);
    return $self->_parse_info_array($raw);
}

sub info_groups {
    my ($self, $stream) = @_;

    croak "stream name required" unless defined $stream && !ref($stream);

    my $raw = $self->_send_command('XINFO', 'GROUPS', $stream);
    return [ map { $self->_parse_info_array($_) } @$raw ];
}

sub info_consumers {
    my ($self, $stream, $group) = @_;

    croak "stream name required" unless defined $stream && !ref($stream);
    croak "group name required" unless defined $group && !ref($group);

    my $raw = $self->_send_command('XINFO', 'CONSUMERS', $stream, $group);
    return [ map { $self->_parse_info_array($_) } @$raw ];
}

# ---

sub auto_claim {
    my ($self, $stream, $group, $consumer, $min_idle_time, $start_id, $count) = @_;

    croak "stream required" unless defined $stream && !ref($stream);
    croak "group required" unless defined $group && !ref($group);
    croak "consumer required" unless defined $consumer && !ref($consumer);
    croak "min_idle_time required" unless defined $min_idle_time && !ref($min_idle_time);

    $start_id //= '0-0';
    croak "start_id must be scalar" if ref($start_id);
    croak "count must be scalar" if defined($count) && ref($count);

    my @args = ($stream, $group, $consumer, $min_idle_time, $start_id);
    push @args, ('COUNT', $count) if defined $count;

    my $raw = $self->_send_command('XAUTOCLAIM', @args);

    my ($next_start_id, $entries) = @$raw;
    my $messages = $self->_parse_entries($entries);

    return ($next_start_id, $messages);
}

# --- Private ---

sub _parse_info_array {
    my ($self, $arrayref) = @_;
    my %info;

    while (my ($k, $v) = splice(@$arrayref, 0, 2) ) {
        $info{$k} = $v;
    }

    return \%info;
}

sub _send_command {
    my ($self, @cmd) = @_;

    $self->_connect;

    warn "cmd: " . join " ", @cmd;

    my $raw = $self->_format_command(@cmd);

    #warn "raw: " . $raw; 

    $self->_send_raw($raw);

    return $self->_parse_response;
}

sub len_stream {
    my ($self, $stream) = @_;

    croak "stream required" unless defined $stream && !ref($stream);

    my $len = $self->_send_command('XLEN', $stream);

    return $len + 0;  # Явно привожу к числу
}

sub _parse_entries {
    my ($self, $entries) = @_;
warn Dumper $entries;
    return [] unless $entries && ref($entries) eq 'ARRAY';

    

    my @result;
    foreach my $entry (@$entries) {
        my ($id, $kv_pairs) = @$entry;
        my %fields;
        while (my ($k, $v) = splice(@$kv_pairs, 0, 2) ) {
            $fields{$k} = $v;
        }
        push @result, { id => $id, message => \%fields };
    }

    return \@result;
}

sub _send_raw {
    my ($self, $raw) = @_;

    $self->_connect;

    my $sock = $self->{sock};

    print $sock $raw or croak "Failed to send raw data: $!";
}

sub _format_command {
    my ($self, @parts) = @_;

    my $out = '*' . scalar(@parts) . "\r\n";
    for my $part (@parts) {
        $part = '' unless defined $part;
        $out .= '$' . length($part) . "\r\n" . $part . "\r\n";
    }
    return $out;
}

sub _parse_response {
    my ($self) = @_;

    my $sock = $self->{sock};

    my $line = _read_line($sock);
    my $prefix = substr($line, 0, 1);
    my $payload = substr($line, 1);

    warn "response: $line";

    if ($prefix eq '+') {
        return $payload;  # simple string
    }
    elsif ($prefix eq '-') {
        croak "Redis error: $payload";
    }
    elsif ($prefix eq ':') {
        return int($payload);  # integer
    }
    elsif ($prefix eq '$') {
        my $len = int($payload);
        return undef if $len == -1; # NULL bulk string

        my $data;
        $sock->read($data, $len);
        _read_line($sock); # read trailing \r\n
        return $data;
    }
    elsif ($prefix eq '*') {
        my $count = int($payload);
        return undef if $count == -1; # NULL array

        my @items;
        for (1..$count) {
            push @items, $self->_parse_response;
        }
        return \@items;
    }
    else {
        croak "Unknown RESP prefix: $prefix";
    }
}

sub _read_line {
    my ($sock) = @_;
    my $line = '';

    while (1) {
        my $char;
        my $read = $sock->read($char, 1);
        croak "Socket closed" unless $read;
        $line .= $char;
        last if $line =~ /\r\n$/;
    }

    $line =~ s/\r\n$//;  # удаляем \r\n
    return $line;
}

sub _connect {
    my ($self, $host, $port) = @_;

    return if $self->{sock};  # Уже подключены

    $host //=  '127.0.0.1';
    $port //= 6379;

    my $sock = IO::Socket::INET->new(
        PeerHost => $host,
        PeerPort => $port,
        Proto    => 'tcp',
        Timeout  => 5,
    ) or croak "Cannot connect to Redis at $host:$port: $!";

    return $sock;
}


1;