use v5.14;
use strict;
use warnings;
use Test::More tests => 8;
use Redis::Streams;

BEGIN {
    package Redis::Streams::Testable;
    use parent 'Redis::Streams';
    our @captured;

    sub _send_command {
        my ($self, @args) = @_;
        push @captured, \@args;

        return [
            [ '1680000000000-0', 'consumer-1', 5000, 1 ],
            [ '1680000000001-0', 'consumer-2', 3000, 2 ],
        ];
    }
}

my $streams = Redis::Streams::Testable->new();
ok($streams, 'Created testable Redis::Streams object');

# Ошибка: нет стрима
{
    my $error;
    eval { $streams->pending_messages(undef, 'group') };
    $error = $@;
    ok($error, 'Dies if no stream');
    like($error, qr/stream name required/i, 'Correct error for missing stream');
}

# Ошибка: нет группы
{
    my $error;
    eval { $streams->pending_messages('stream', undef) };
    $error = $@;
    ok($error, 'Dies if no group');
    like($error, qr/group name required/i, 'Correct error for missing group');
}

# Успешный запрос
@Redis::Streams::Testable::captured = ();
my $pending = $streams->pending_messages('stream', 'group');

ok(ref($pending) eq 'ARRAY', 'Returns arrayref');
is(scalar @$pending, 2, 'Got two pending messages');

my $cmd = join ' ', @{$Redis::Streams::Testable::captured[0]};
like($cmd, qr/XPENDING stream group - \+ 10/, 'Correct XPENDING command sent');