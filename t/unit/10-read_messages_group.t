use v5.14;
use strict;
use warnings;
use Test::More tests => 7;
use Redis::Streams;

BEGIN {
    package Redis::Streams::Testable;
    use parent 'Redis::Streams';
    our @captured;

    sub _send_command {
        my ($self, @args) = @_;
        push @captured, \@args;

        return [
            [ 'orders-stream', [
                [ '1680000000000-0', ['foo', 'bar'] ]
            ] ]
        ];
    }
}

my $streams = Redis::Streams::Testable->new();
ok($streams, 'Created testable Redis::Streams object');

# --- Ошибка: нет группы
{
    my $error;
    eval { $streams->read_messages_group(undef, 'consumer', 'stream') };
    $error = $@;
    ok($error, 'Dies if no group');
    like($error, qr/group name required/i, 'Correct error for missing group');
}

# --- Ошибка: нет консьюмера
{
    my $error;
    eval { $streams->read_messages_group('group', undef, 'stream') };
    $error = $@;
    ok($error, 'Dies if no consumer');
    like($error, qr/consumer name required/i, 'Correct error for missing consumer');
}

# --- Успешное чтение
@Redis::Streams::Testable::captured = ();
my $messages = $streams->read_messages_group('group', 'consumer', 'orders-stream');
ok(ref($messages) eq 'ARRAY', 'Returns arrayref');

my $cmd = join ' ', @{$Redis::Streams::Testable::captured[0]};
like($cmd, qr/XREADGROUP GROUP group consumer STREAMS orders-stream >/i, 'Correct XREADGROUP command');