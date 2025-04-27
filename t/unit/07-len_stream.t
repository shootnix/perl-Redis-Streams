use v5.14;
use strict;
use warnings;
use Test::More tests => 4;
use Redis::Streams;

BEGIN {
    package Redis::Streams::Testable;
    use parent 'Redis::Streams';
    our @captured;

    sub _send_command {
        my ($self, @args) = @_;
        push @captured, \@args;
        return 42;  # Мокаем ответ Redis на XLEN
    }
}

my $streams = Redis::Streams::Testable->new();
ok($streams, 'Created testable Redis::Streams object');

# --- Проверка правильного вызова
@Redis::Streams::Testable::captured = ();

my $len = $streams->len_stream('teststream');

is($len, 42, 'len_stream returns correct mocked value');

is_deeply(
    $Redis::Streams::Testable::captured[0],
    ['XLEN', 'teststream'],
    'Captured correct XLEN command'
);

# --- Ошибка при отсутствии имени стрима
eval { $streams->len_stream(undef) };
like($@, qr/stream required/, 'Dies if stream name is missing');