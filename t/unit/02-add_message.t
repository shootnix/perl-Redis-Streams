use v5.14;
use strict;
use warnings;
use Test::More tests => 25;
use Redis::Streams;

BEGIN {
    package Redis::Streams::Testable;
    use parent 'Redis::Streams';
    our @captured;

    sub _send_command {
        my ($self, @data) = @_;
        push @captured, \@data;
        return '1680000000000-0';  # Мокаем реальный ID
    }

    sub _connect {}
}

# --- ИНИЦИАЛИЗАЦИЯ ---
my $streams = Redis::Streams::Testable->new();
ok($streams, 'Created testable Redis::Streams object');

# --- ТЕСТЫ ---

sub test_missing_stream_name {
    my $error;
    eval { $streams->add_message(undef, { foo => 'bar' }) };
    $error = $@;
    ok($error, 'Dies when stream name is missing');
    like($error, qr/stream name required/i, 'Error says stream name required');
}

sub test_missing_message {
    my $error;
    eval { $streams->add_message('teststream', undef) };
    $error = $@;
    ok($error, 'Dies when message is missing');
    like($error, qr/message must be a hash reference/i, 'Error says message must be hashref');
}

sub test_message_not_hashref {
    my $error;
    eval { $streams->add_message('teststream', ['foo', 'bar']) };
    $error = $@;
    ok($error, 'Dies when message is not a hashref');
    like($error, qr/message must be a hash reference/i, 'Error says message must be hashref');
}

sub test_message_with_non_scalar_value {
    my $error;
    eval { $streams->add_message('teststream', { foo => { bar => 'baz' } }) };
    $error = $@;
    ok($error, 'Dies when non-scalar value given');
    like($error, qr/cannot add non-scalar value/i, 'Error says non-scalar value');
}

sub test_add_empty_message {
    @Redis::Streams::Testable::captured = ();
    my $id = $streams->add_message('emptystream', {});
    ok(defined $id, "add_message with empty hash returns ID");

    my $sent = join ' ', @{$Redis::Streams::Testable::captured[0]};
    like($sent, qr/XADD/, 'Captured XADD for empty hash');
}

sub test_add_simple_message_auto_id {
    @Redis::Streams::Testable::captured = ();
    my $id = $streams->add_message('teststream', { foo => 'bar', baz => 'qux' });
    ok(defined $id, "add_message with simple hash returns ID");

    my @args = @{$Redis::Streams::Testable::captured[0]};
    is($args[0], 'XADD', 'Command is XADD');
    is($args[1], 'teststream', 'Stream name is correct');
    is($args[2], '*', 'ID is *');

    # Проверяем, что все пары ключ-значение есть
    my %got;
    @got{@args[3..$#args]} = ();

    ok(exists $got{'foo'}, 'Field foo is present');
    ok(exists $got{'bar'}, 'Field bar is present');
    ok(exists $got{'baz'}, 'Field baz is present');
    ok(exists $got{'qux'}, 'Field qux is present');
}

sub test_add_simple_message_custom_id {
    @Redis::Streams::Testable::captured = ();
    my $id = $streams->add_message('teststream', { foo => 'bar' }, { id => '1234567890-0' });
    ok(defined $id, "add_message with custom id returns ID");

    my @args = @{$Redis::Streams::Testable::captured[0]};
    is($args[0], 'XADD', 'Command is XADD');
    is($args[1], 'teststream', 'Stream name is correct');
    is($args[2], '1234567890-0', 'ID is 1234567890-0');

    my %got;
    @got{@args[3..$#args]} = ();

    ok(exists $got{'foo'}, 'Field foo is present');
    ok(exists $got{'bar'}, 'Field bar is present');
}

# --- ВЫЗОВ ТЕСТОВ ---
test_missing_stream_name();
test_missing_message();
test_message_not_hashref();
test_message_with_non_scalar_value();
test_add_empty_message();
test_add_simple_message_auto_id();
test_add_simple_message_custom_id();