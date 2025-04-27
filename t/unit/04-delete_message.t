use v5.14;
use strict;
use warnings;
use Test::More tests => 7;
use Redis::Streams;

BEGIN {
    package Redis::Streams::Testable;
    use parent 'Redis::Streams';
    our @captured;
    our $mocked_response;

    sub _send_command {
        my ($self, @data) = @_;
        push @captured, \@data;
        return $mocked_response // '+OK'; # Мокаем успешный ответ
    }

    sub _connect {}
}

# --- Тесты ---

my $streams = Redis::Streams::Testable->new();
ok($streams, 'Created testable Redis::Streams object');

@Redis::Streams::Testable::captured = ();

# Ошибка если нет стрима
{
    my $error;
    eval { $streams->delete_message(undef, '1680000000000-0') };
    $error = $@;
    ok($error, 'Dies if stream is missing');
    like($error, qr/stream required/i, 'Error mentions stream required');
}

# Ошибка если нет id
{
    my $error;
    eval { $streams->delete_message('mystream', undef) };
    $error = $@;
    ok($error, 'Dies if id is missing');
    like($error, qr/id required/i, 'Error mentions id required');
}

# Ок: удаление одного сообщения
{
    @Redis::Streams::Testable::captured = ();
    my $result = $streams->delete_message('mystream', '1680000000000-0');

    my @sent = @{$Redis::Streams::Testable::captured[0]};
    is_deeply(
        \@sent,
        ['XDEL', 'mystream', '1680000000000-0'],
        'Sent correct XDEL command'
    );
}

# Ок: удаление нескольких сообщений
{
    @Redis::Streams::Testable::captured = ();
    my $result = $streams->delete_message('mystream', ['1680000000000-0', '1680000000001-0', '1680000000002-0']);

    my @sent = @{$Redis::Streams::Testable::captured[0]};
    is_deeply(
        \@sent,
        ['XDEL', 'mystream', '1680000000000-0', '1680000000001-0', '1680000000002-0'],
        'Sent correct XDEL command for multiple ids'
    );
}