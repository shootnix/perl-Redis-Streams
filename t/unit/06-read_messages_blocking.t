use v5.14;
use strict;
use warnings;
use Test::More tests => 6;
use Redis::Streams;

BEGIN {
    package Redis::Streams::Testable;
    use parent 'Redis::Streams';
    our @captured;

    sub _send_command {
        my ($self, @args) = @_;
        push @captured, \@args;

        # Мокаем стандартный ответ для XREAD BLOCK
        return [
            [
                'teststream', [
                    ['1000-0', ['foo', 'bar']],
                ]
            ]
        ];
    }
}

my $streams = Redis::Streams::Testable->new();
ok($streams, 'Created testable Redis::Streams object');

# --- Вызов без last_id и timeout (оба дефолтные)
@Redis::Streams::Testable::captured = ();

my $messages = $streams->read_messages_blocking('teststream');

ok($messages && ref($messages) eq 'ARRAY', 'read_messages_block returns arrayref');

is_deeply(
    $Redis::Streams::Testable::captured[0],
    ['XREAD', 'BLOCK', 5000, 'STREAMS', 'teststream', '$'],
    'Captured correct XREAD BLOCK command with defaults'
);

is($messages->[0]->{id}, '1000-0', 'First message id correct');
is_deeply($messages->[0]->{message}, { foo => 'bar' }, 'First message content correct');

# --- Вызов с указанными last_id и timeout
@Redis::Streams::Testable::captured = ();

$messages = $streams->read_messages_blocking('teststream', '5000-0', 10000);

is_deeply(
    $Redis::Streams::Testable::captured[0],
    ['XREAD', 'BLOCK', 10000, 'STREAMS', 'teststream', '5000-0'],
    'Captured XREAD BLOCK with custom last_id and timeout'
);