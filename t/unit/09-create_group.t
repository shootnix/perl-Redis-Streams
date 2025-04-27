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
        return 'OK';  # Мокаем успешный ответ
    }
}

my $streams = Redis::Streams::Testable->new();
ok($streams, 'Created testable Redis::Streams object');

# --- Ошибка: Нет стрима
{
    my $error;
    eval { $streams->create_group(undef, 'mygroup') };
    $error = $@;
    ok($error, 'Dies when stream name is missing');
    like($error, qr/stream name required/i, 'Correct error for missing stream');
}

# --- Ошибка: Нет группы
{
    my $error;
    eval { $streams->create_group('mystream', undef) };
    $error = $@;
    ok($error, 'Dies when group name is missing');
    like($error, qr/group name required/i, 'Correct error for missing group');
}

# --- Успешное создание
@Redis::Streams::Testable::captured = ();
my $ok = $streams->create_group('mystream', 'mygroup');
ok($ok, 'create_group returns success');

my $sent = join ' ', @{$Redis::Streams::Testable::captured[0]};
like($sent, qr/XGROUP CREATE mystream mygroup \$/, 'Correct XGROUP CREATE command sent');