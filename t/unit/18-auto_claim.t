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
        return [
            '1745749000000-0',
            [
                [ '1745748000000-0', [ 'foo', 'bar' ] ],
                [ '1745748100000-0', [ 'baz', 'qux' ] ]
            ]
        ];
    }

    sub _parse_entries {
        my ($self, $entries) = @_;
        return [
            { id => '1745748000000-0', fields => { foo => 'bar' } },
            { id => '1745748100000-0', fields => { baz => 'qux' } },
        ];
    }
}

my $streams = Redis::Streams::Testable->new();
ok($streams, 'Created Redis::Streams object');

# Ошибки
{
    my $error;
    eval { $streams->auto_claim(undef, 'group', 'consumer', 60000) };
    like($@, qr/stream required/, 'No stream');
}
{
    my $error;
    eval { $streams->auto_claim('stream', undef, 'consumer', 60000) };
    like($@, qr/group required/, 'No group');
}
{
    my $error;
    eval { $streams->auto_claim('stream', 'group', undef, 60000) };
    like($@, qr/consumer required/, 'No consumer');
}
{
    my $error;
    eval { $streams->auto_claim('stream', 'group', 'consumer', undef) };
    like($@, qr/min_idle_time required/, 'No min_idle_time');
}

# Успех
@Redis::Streams::Testable::captured = ();
my ($next_id, $messages) = $streams->auto_claim('stream', 'group', 'consumer', 60000, '0-0', 100);

ok($next_id eq '1745749000000-0', 'Correct next start ID');
ok(ref($messages) eq 'ARRAY', 'Messages is ARRAY');
is(scalar(@$messages), 2, 'Two messages returned');

my $cmd = join ' ', @{$Redis::Streams::Testable::captured[0]};
like($cmd, qr/XAUTOCLAIM stream group consumer 60000 0-0 COUNT 100/, 'Correct XAUTOCLAIM command sent');