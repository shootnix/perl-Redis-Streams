use v5.14;
use strict;
use warnings;
use Test::More tests => 5;
use Redis::Streams;

BEGIN {
    package Redis::Streams::Testable;
    use parent 'Redis::Streams';
    our @captured;

    sub _send_command {
        my ($self, @args) = @_;
        push @captured, \@args;
        return 1; # Мокаем успешное удаление
    }
}

my $streams = Redis::Streams::Testable->new();
ok($streams, 'Created testable Redis::Streams object');

# Ошибки
{
    my $error;
    eval { $streams->delete_group(undef, 'group') };
    like($@, qr/stream name required/, 'No stream');
}
{
    my $error;
    eval { $streams->delete_group('stream', undef) };
    like($@, qr/group name required/, 'No group');
}

# Успех
@Redis::Streams::Testable::captured = ();
my $deleted = $streams->delete_group('stream', 'group');
ok($deleted == 1, 'delete_group returns 1');

my $cmd = join ' ', @{$Redis::Streams::Testable::captured[0]};
like($cmd, qr/XGROUP DESTROY stream group/, 'Correct XGROUP DESTROY command sent');