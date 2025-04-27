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
        return 42;  # Мокаем, что удалено 42 сообщения
    }
}

my $streams = Redis::Streams::Testable->new();
ok($streams, 'Created testable Redis::Streams object');

# --- Ошибка: Нет имени стрима
{
    my $error;
    eval { $streams->trim_stream(undef, 100) };
    $error = $@;
    ok($error, 'Dies when stream name is missing');
    like($error, qr/stream name required/i, 'Correct error message for missing stream');
}

# --- Ошибка: Неверный maxlen
{
    my $error;
    eval { $streams->trim_stream('teststream', 'not_a_number') };
    $error = $@;
    ok($error, 'Dies when maxlen is invalid');
    like($error, qr/maxlen must be a positive integer/i, 'Correct error message for invalid maxlen');
}

# --- Ок: Обрезка работает
@Redis::Streams::Testable::captured = ();
my $deleted = $streams->trim_stream('teststream', 1000);

ok(defined $deleted && $deleted == 42, 'trim_stream returns number of deleted entries');

my $sent = join ' ', @{$Redis::Streams::Testable::captured[0]};
like($sent, qr/XTRIM teststream MAXLEN 1000/, 'Correct XTRIM command sent');