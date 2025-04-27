use v5.14;
use strict;
use warnings;
use Test::More tests => 10;
use Redis::Streams;

BEGIN {
    package Redis::Streams::Testable;
    use parent 'Redis::Streams';
    our @captured;
    our $mocked_response;

    sub _send_command {
        my ($self, @data) = @_;
        push @captured, \@data;
        return $mocked_response // [];
    }

    sub _connect {}
}

# Инициализируем наш тестовый объект
my $streams = Redis::Streams::Testable->new();
ok($streams, 'Created testable Redis::Streams object');

# Очищаем капчур перед каждым тестом
@Redis::Streams::Testable::captured = ();

# --- Тест: Ошибка если нет стрима
{
    my $error;
    eval { $streams->read_messages(undef) };
    $error = $@;
    ok($error, 'Dies if stream is missing');
    like($error, qr/stream required/, 'Correct error for missing stream');
}

# --- Тест: Стандартное чтение без указания last_id
{
    @Redis::Streams::Testable::captured = ();
    $Redis::Streams::Testable::mocked_response = [
        ['mystream', [
            ['1680000000000-0', ['foo', 'bar', 'baz', 'qux']],
            ['1680000000001-0', ['hello', 'world']]
        ]]
    ];

    my $messages = $streams->read_messages('mystream');

    ok(ref($messages) eq 'ARRAY', 'Got arrayref as response');
    is(scalar @$messages, 2, 'Got two messages');

    is($messages->[0]->{id}, '1680000000000-0', 'First message id correct');
    is_deeply($messages->[0]->{message}, { foo => 'bar', baz => 'qux' }, 'First message fields correct');

    is($messages->[1]->{id}, '1680000000001-0', 'Second message id correct');
    is_deeply($messages->[1]->{message}, { hello => 'world' }, 'Second message fields correct');

    my @sent = @{$Redis::Streams::Testable::captured[0]};
    is_deeply(
        \@sent,
        ['XREAD', 'STREAMS', 'mystream', '0'],
        'Correct XREAD command sent'
    );
}