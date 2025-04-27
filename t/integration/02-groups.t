use v5.14;
use strict;
use warnings;
use Test::More tests => 15;
use Redis::Streams;

my $streams = Redis::Streams->new();

ok($streams, 'Connected to Redis');

# Очистим стрим
$streams->_send_command('DEL', 'groupstream');

# Добавляем хотя бы одно сообщение ДО создания группы
my $initial_id = $streams->add_message('groupstream', { foo => 'init' });
ok($initial_id, 'Added initial message');

# Создаем группу
ok($streams->create_group('groupstream', 'testgroup', $initial_id), 'Group created');

# Добавляем рабочие сообщения
my $id1 = $streams->add_message('groupstream', { foo => 'bar' });
ok($id1, 'Added message 1');

my $id2 = $streams->add_message('groupstream', { foo => 'baz' });
ok($id2, 'Added message 2');

# Читаем сообщения через группу
my $messages = $streams->read_messages_group('groupstream', 'testgroup', 'consumer1');

ok(ref($messages) eq 'ARRAY', 'Read messages from group');
is(scalar(@$messages), 2, 'Read two messages');

# Проверяем содержимое
is($messages->[0]->{message}->{foo}, 'bar', 'First message foo=bar');
is($messages->[1]->{message}->{foo}, 'baz', 'Second message foo=baz');

# Ack'аем первое сообщение
my $ack = $streams->ack_message('groupstream', 'testgroup', $messages->[0]->{id});
ok($ack >= 1, 'First message acknowledged');

# Проверяем pending
my $pending = $streams->pending_messages('groupstream', 'testgroup');
ok(ref($pending) eq 'ARRAY', 'Pending messages returned');
is(scalar(@$pending), 1, 'One pending message remains');

# Sleep для авто-клейма
sleep(1);

# Авто-клейм
my ($next_id, $claimed) = $streams->auto_claim('groupstream', 'testgroup', 'consumer2', 0, '0-0', 10);

ok(defined $next_id, 'Next ID returned');
ok(ref($claimed) eq 'ARRAY', 'Claimed messages array');
is(scalar(@$claimed), 1, 'Claimed one message');
is($claimed->[0]->{message}->{foo}, 'baz', 'Claimed message has correct data');