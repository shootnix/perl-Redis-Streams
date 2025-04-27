use v5.14;
use strict;
use warnings;
use Test::More tests => 4;
use Redis::Streams;

BEGIN {
    package Redis::Streams::Testable;
    use parent 'Redis::Streams';
    our @captured;
    our $mocked_response;

    sub _send_command {
        my ($self, @data) = @_;
        push @captured, \@data;
        return $mocked_response // '+OK';
    }

    sub _connect {}
}

my $streams = Redis::Streams::Testable->new();
ok($streams, 'Created testable Redis::Streams object');

@Redis::Streams::Testable::captured = ();

# Ошибка если нет стрима
{
    my $error;
    eval { $streams->delete_stream(undef) };
    $error = $@;
    ok($error, 'Dies if stream is missing');
    like($error, qr/stream required/i, 'Error mentions stream required');
}

# Ок: удаление стрима
{
    @Redis::Streams::Testable::captured = ();
    my $result = $streams->delete_stream('mystream');

    my @sent = @{$Redis::Streams::Testable::captured[0]};
    is_deeply(
        \@sent,
        ['DEL', 'mystream'],
        'Sent correct DEL command'
    );
}