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
        return 1; # Redis вернёт количество удалённых pending-сообщений
    }
}

my $streams = Redis::Streams::Testable->new();
ok($streams, 'Created testable Redis::Streams object');

# Ошибки
{
    my $error;
    eval { $streams->delete_consumer(undef, 'group', 'consumer') };
    like($@, qr/stream name required/, 'No stream');
}
{
    my $error;
    eval { $streams->delete_consumer('stream', undef, 'consumer') };
    like($@, qr/group name required/, 'No group');
}
{
    my $error;
    eval { $streams->delete_consumer('stream', 'group', undef) };
    like($@, qr/consumer name required/, 'No consumer');
}

# Успех
@Redis::Streams::Testable::captured = ();
my $deleted = $streams->delete_consumer('stream', 'group', 'consumer');
ok($deleted == 1, 'delete_consumer returns 1');

my $cmd = join ' ', @{$Redis::Streams::Testable::captured[0]};
like($cmd, qr/XGROUP DELCONSUMER stream group consumer/, 'Correct XGROUP DELCONSUMER command');