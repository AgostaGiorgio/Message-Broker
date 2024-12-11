import pytest
from message_broker_v2 import MessageBroker, TopicRepository


@pytest.fixture(autouse=True)
def reset_singleton():
    TopicRepository._instance = None

def test_create_topic():
    msg_brk = MessageBroker()
    msg_brk2 = MessageBroker()
    topic_created = msg_brk.create_topic("topic name")
    topic_created2 = msg_brk2.create_topic("topic name 2")

    assert msg_brk.topic_repository is msg_brk2.topic_repository
    assert topic_created == "topic name"
    assert topic_created2 == "topic name 2"
    assert "topic name" in msg_brk.topic_repository._topics and "topic name" in msg_brk2.topic_repository._topics
    assert "topic name 2" in msg_brk.topic_repository._topics and "topic name 2" in msg_brk2.topic_repository._topics
    assert msg_brk.topic_repository.topic_count() == 2 and msg_brk2.topic_repository.topic_count() == 2


def test_create_topic_twice():
    msg_brk = MessageBroker()
    msg_brk2 = MessageBroker()
    topic_created = msg_brk.create_topic("topic name")
    topic_created2 = msg_brk2.create_topic("topic name")

    assert msg_brk.topic_repository is msg_brk2.topic_repository
    assert topic_created == topic_created2 == "topic name"
    assert "topic name" in msg_brk.topic_repository._topics and "topic name" in msg_brk2.topic_repository._topics
    assert msg_brk.topic_repository.topic_count() == 1 and msg_brk2.topic_repository.topic_count() == 1


def test_create_topic_invalid_name():
    msg_brk = MessageBroker()
    
    with pytest.raises(Exception, match="Topic name cannot be None"):
        msg_brk.create_topic(None)
    assert not msg_brk.topic_repository._topics


def test_publish_message():
    msg_brk = MessageBroker()
    msg_brk2 = MessageBroker()
    topic_name = msg_brk.create_topic("topic name")
    msg_brk.publish(topic_name, "message to publish")
    msg_brk2.publish(topic_name, "message to publish 2")

    assert msg_brk.topic_repository is msg_brk2.topic_repository
    assert "topic name" in msg_brk.topic_repository._topics and msg_brk.topic_repository.topic_count() == 1
    assert "topic name" in msg_brk2.topic_repository._topics and msg_brk2.topic_repository.topic_count() == 1
    assert len(msg_brk.topic_repository._topics[topic_name]) == 2
    assert len(msg_brk2.topic_repository._topics[topic_name]) == 2


def test_publish_message_none():
    msg_brk = MessageBroker()
    msg_brk2 = MessageBroker()
    topic_name = msg_brk.create_topic("topic name")
    msg_brk.publish(topic_name, "message to publish")

    with pytest.raises(Exception, match="Message cannot be None"):
        msg_brk2.publish(topic_name, None)

    assert msg_brk.topic_repository is msg_brk2.topic_repository
    assert "topic name" in msg_brk.topic_repository._topics and msg_brk.topic_repository.topic_count() == 1
    assert "topic name" in msg_brk2.topic_repository._topics and msg_brk2.topic_repository.topic_count() == 1
    assert len(msg_brk.topic_repository._topics[topic_name]) == 1
    assert len(msg_brk2.topic_repository._topics[topic_name]) == 1


def test_publish_message_invalid_topic():
    msg_brk = MessageBroker()
    msg_brk2 = MessageBroker()
    topic_name = msg_brk.create_topic("topic name")
    msg_brk.publish(topic_name, "message to publish")

    with pytest.raises(Exception, match="Topic 'topic name 2' doesn't exist"):
        msg_brk2.publish("topic name 2", "message to publish 2")
    
    assert msg_brk.topic_repository is msg_brk2.topic_repository
    assert "topic name" in msg_brk.topic_repository._topics and msg_brk.topic_repository.topic_count() == 1
    assert "topic name" in msg_brk2.topic_repository._topics and msg_brk2.topic_repository.topic_count() == 1
    assert len(msg_brk.topic_repository._topics[topic_name]) == 1
    assert len(msg_brk2.topic_repository._topics[topic_name]) == 1


def test_subscribe_to_topic():
    msg_brk = MessageBroker()
    msg_brk2 = MessageBroker()
    topic_name = msg_brk.create_topic("topic name")
    offset = msg_brk2.subscribe(topic_name)

    assert msg_brk.topic_repository is msg_brk2.topic_repository
    assert "topic name" in msg_brk.topic_repository._topics and msg_brk.topic_repository.topic_count() == 1
    assert "topic name" in msg_brk2.topic_repository._topics and msg_brk2.topic_repository.topic_count() == 1
    assert len(msg_brk.topic_repository._topics[topic_name]) == 0
    assert len(msg_brk2.topic_repository._topics[topic_name]) == 0
    assert msg_brk2.id in msg_brk2.topic_repository._subscribers[topic_name]
    assert msg_brk2.topic_repository._subscribers[topic_name][msg_brk2.id] == 0
    assert offset == 0


def test_subscribe_to_unexisting_topic():
    msg_brk = MessageBroker()

    with pytest.raises(Exception, match="Topic 'topic name' doesn't exist"):
        msg_brk.subscribe("topic name")
    assert "topic name" not in msg_brk.topic_repository._subscribers
    assert msg_brk.topic_repository.topic_count() == 0


def test_subscribe_to_topic_twice():
    msg_brk = MessageBroker()
    topic_name = msg_brk.create_topic("topic name")
    offset = msg_brk.subscribe(topic_name)
    offset = msg_brk.subscribe(topic_name)

    assert offset == 0
    assert topic_name in msg_brk.topic_repository._subscribers
    assert msg_brk.id in msg_brk.topic_repository._subscribers[topic_name]


def test_consume_from_topic():
    msg_brk = MessageBroker()
    msg_brk2 = MessageBroker()
    topic_name = msg_brk.create_topic("topic name")
    msg_brk.subscribe(topic_name)
    msg_brk2.subscribe(topic_name)
    msg_brk.publish(topic_name, "message to publish 1")
    msg11 = msg_brk.consume(topic_name)
    msg_brk.publish(topic_name, "message to publish 2")
    msg12 = msg_brk.consume(topic_name)
    msg_brk2.publish(topic_name, "message to publish 3")
    msg13 = msg_brk.consume(topic_name)
    msg21 = msg_brk2.consume(topic_name)
    msg22 = msg_brk2.consume(topic_name)
    msg23 = msg_brk2.consume(topic_name)

    assert msg11 == "message to publish 1"
    assert msg12 == "message to publish 2"
    assert msg13 == "message to publish 3"
    assert msg21 == "message to publish 1"
    assert msg22 == "message to publish 2"
    assert msg23 == "message to publish 3"


def test_consume_from_empty_topic():
    msg_brk = MessageBroker()
    msg_brk2 = MessageBroker()
    topic_name = msg_brk.create_topic("topic name")
    msg_brk2.subscribe(topic_name)
    msg1 = msg_brk2.consume(topic_name)

    assert msg1 == None


def test_consume_from_unknown_topic():
    msg_brk = MessageBroker()

    with pytest.raises(Exception, match="Topic 'topic name' doesn't exist"):
        msg_brk.consume("topic name")

