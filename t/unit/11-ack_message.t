use v5.14;
use strict;
use warnings;
use Test::More tests => 9;
use Redis::Streams;

BEGIN {
    package Redis::Streams::Testable;
    use parent 'Redis::Streams';
    our @captured;

    sub _send_command {
        my ($self, @args) = @_;
        push @captured, \@args;
        return 1;  # Redis возвращает количество подтвержденных сообщений
    }
}

my $streams = Redis::Streams::Testable->new();
ok($streams, 'Created testable Redis::Streams object');

# Ошибка: нет стрима
{
    my $error;
    eval { $streams->ack_message(undef, 'group', '1234-0') };
    $error = $@;
    ok($error, 'Dies if no stream');
    like($error, qr/stream name required/i, 'Correct error for missing stream');
}

# Ошибка: нет группы
{
    my $error;
    eval { $streams->ack_message('stream', undef, '1234-0') };
    $error = $@;
    ok($error, 'Dies if no group');
    like($error, qr/group name required/i, 'Correct error for missing group');
}

# Ошибка: нет ID
{
    my $error;
    eval { $streams->ack_message('stream', 'group') };
    $error = $@;
    ok($error, 'Dies if no id');
    like($error, qr/at least one id required/i, 'Correct error for missing id');
}

# Успешный ACK
@Redis::Streams::Testable::captured = ();
my $count = $streams->ack_message('stream', 'group', '1234-0', '1234-1');
ok($count == 1, 'ack_message returns success');

my $cmd = join ' ', @{$Redis::Streams::Testable::captured[0]};
like($cmd, qr/XACK stream group 1234-0 1234-1/, 'Correct XACK command sent');