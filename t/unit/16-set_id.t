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
        return "OK";  # Redis возвращает "OK"
    }
}

my $streams = Redis::Streams::Testable->new();
ok($streams, 'Created testable Redis::Streams object');

# Ошибки
{
    my $error;
    eval { $streams->set_id(undef, 'group', '0-0') };
    like($@, qr/stream name required/, 'No stream');
}
{
    my $error;
    eval { $streams->set_id('stream', undef, '0-0') };
    like($@, qr/group name required/, 'No group');
}
{
    my $error;
    eval { $streams->set_id('stream', 'group', undef) };
    like($@, qr/id required/, 'No id');
}

# Успех
@Redis::Streams::Testable::captured = ();
my $ok = $streams->set_id('stream', 'group', '1680000000000-0');
ok($ok eq 'OK', 'set_id returns OK');

my $cmd = join ' ', @{$Redis::Streams::Testable::captured[0]};
like($cmd, qr/XGROUP SETID stream group 1680000000000-0/, 'Correct XGROUP SETID command');