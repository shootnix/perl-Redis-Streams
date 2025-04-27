use v5.40;

use Data::Dumper;
use Redis::Streams;


my $streams = Redis::Streams->new();

warn Dumper $streams->_send_command('PING');

warn Dumper $streams->add_message('foo', { foo => "FOO" });

my $messages = $streams->read_messages('foo');

#$streams->delete_message('foo', [ map { $_->{id} } @$messages ]);

warn $streams->len_stream('foo');