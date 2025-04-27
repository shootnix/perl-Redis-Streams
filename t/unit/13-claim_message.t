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

        # Мокаем: если JUSTID => массив ID, иначе массив записей
        if (grep { $_ eq 'JUSTID' } @args) {
            return ['1680000000000-0', '1680000000001-0'];
        } else {
            return [
                [ 'stream', [
                    [ '1680000000000-0', ['foo', 'bar'] ]
                ] ]
            ];
        }
    }

    sub _parse_entries {
        my ($self, $stream, $raw) = @_;
        return [{ id => '1680000000000-0', message => { foo => 'bar' } }];
    }
}

my $streams = Redis::Streams::Testable->new();
ok($streams, 'Created testable Redis::Streams object');

# Ошибки
{
    my $error;
    eval { $streams->claim_message(undef, 'group', 'consumer', ['id'], 0) };
    like($@, qr/stream name required/, 'No stream');
}
{
    my $error;
    eval { $streams->claim_message('stream', undef, 'consumer', ['id'], 0) };
    like($@, qr/group name required/, 'No group');
}
{
    my $error;
    eval { $streams->claim_message('stream', 'group', undef, ['id'], 0) };
    like($@, qr/consumer name required/, 'No consumer');
}
{
    my $error;
    eval { $streams->claim_message('stream', 'group', 'consumer', undef, 0) };
    like($@, qr/ids must be arrayref/, 'No ids');
}
{
    my $error;
    eval { $streams->claim_message('stream', 'group', 'consumer', [], 0) };
    like($@, qr/ids must be arrayref/, 'Empty ids');
}

# Успешный CLAIM с JUSTID
@Redis::Streams::Testable::captured = ();
my $ids = $streams->claim_message('stream', 'group', 'consumer', ['id1', 'id2'], 5000, { justid => 1 });
ok(ref($ids) eq 'ARRAY', 'Got arrayref of ids');

my $cmd = join ' ', @{$Redis::Streams::Testable::captured[0]};
like($cmd, qr/XCLAIM stream group consumer 5000 id1 id2 JUSTID/, 'Correct XCLAIM JUSTID command');