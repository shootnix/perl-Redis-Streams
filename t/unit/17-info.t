use v5.14;
use strict;
use warnings;
use Test::More tests => 10;
use Redis::Streams;

BEGIN {
    package Redis::Streams::Testable;
    use parent 'Redis::Streams';
    our @captured;

    my %mock_responses = (
        'XINFO STREAM mystream' => [ 'length', 5, 'radix-tree-keys', 1 ],
        'XINFO GROUPS mystream' => [ 
            [ 'name', 'mygroup', 'consumers', 2, 'pending', 0 ],
            [ 'name', 'othergroup', 'consumers', 1, 'pending', 3 ]
        ],
        'XINFO CONSUMERS mystream mygroup' => [
            [ 'name', 'consumer-1', 'pending', 1, 'idle', 1000 ],
            [ 'name', 'consumer-2', 'pending', 0, 'idle', 5000 ]
        ],
    );

    sub _send_command {
        my ($self, @args) = @_;
        push @captured, \@args;
        my $cmd = join ' ', @args;
        return $mock_responses{$cmd};
    }
}

my $streams = Redis::Streams::Testable->new();
ok($streams, 'Created Redis::Streams object');

# STREAM
my $stream_info = $streams->info_stream('mystream');
ok(ref($stream_info) eq 'HASH', 'Stream info is HASH');
is($stream_info->{length}, 5, 'Stream length correct');

# GROUPS
my $groups_info = $streams->info_groups('mystream');
ok(ref($groups_info) eq 'ARRAY', 'Groups info is ARRAY');
is(scalar(@$groups_info), 2, 'Two groups found');
is($groups_info->[0]->{name}, 'mygroup', 'First group is mygroup');

# CONSUMERS
my $consumers_info = $streams->info_consumers('mystream', 'mygroup');
ok(ref($consumers_info) eq 'ARRAY', 'Consumers info is ARRAY');
is(scalar(@$consumers_info), 2, 'Two consumers found');
is($consumers_info->[0]->{name}, 'consumer-1', 'First consumer is consumer-1');

# Проверка захваченной команды
my $sent = join ' ', @{$Redis::Streams::Testable::captured[-1]};
like($sent, qr/XINFO CONSUMERS mystream mygroup/, 'Correct XINFO CONSUMERS sent');