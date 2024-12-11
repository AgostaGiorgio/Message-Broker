import pytest
from message_broker_v1 import MessageBroker


def test_create_topic():
    msg_brk = MessageBroker()
    topic_created = msg_brk.create_topic("topic name")

    assert topic_created == "topic name"
    assert "topic name" in msg_brk._topics and len(msg_brk._topics) == 1


def test_create_topic_twice():
    msg_brk = MessageBroker()
    msg_brk.create_topic("topic name")
    msg_brk.create_topic("topic name")
    assert "topic name" in msg_brk._topics and len(msg_brk._topics) == 1


def test_create_topic_invalid_name():
    msg_brk = MessageBroker()
    
    with pytest.raises(Exception, match="Topic name cannot be None"):
        msg_brk.create_topic(None)
    assert not msg_brk._topics

def test_publish_message():
    msg_brk = MessageBroker()
    topic_name = msg_brk.create_topic("topic name")
    msg_brk.publish(topic_name, "message to publish")
    assert "topic name" in msg_brk._topics and len(msg_brk._topics) == 1
    assert msg_brk._topics[topic_name].qsize() == 1


def test_publish_message_none():
    msg_brk = MessageBroker()
    topic_name = msg_brk.create_topic("topic name")

    with pytest.raises(Exception, match="Message cannot be None"):
        msg_brk.publish(topic_name, None)
    assert "topic name" in msg_brk._topics and len(msg_brk._topics) == 1
    assert msg_brk._topics[topic_name].empty()


def test_publish_message_invalid_topic():
    msg_brk = MessageBroker()

    with pytest.raises(Exception, match="Topic 'topic name' doesn't exist"):
        msg_brk.publish("topic name", "message to publish")
    assert len(msg_brk._subscribed) == 0
    assert len(msg_brk._subscribed) == 0
    assert not msg_brk._topics


def test_subscribe_to_topic():
    msg_brk = MessageBroker()
    msg_brk.create_topic("topic name")
    msg_brk.subscribe("topic name")
    assert "topic name" in msg_brk._subscribed and len(msg_brk._subscribed) == 1
    assert "topic name" in msg_brk._topics and len(msg_brk._topics) == 1


def test_subscribe_to_unexisting_topic():
    msg_brk = MessageBroker()

    with pytest.raises(Exception, match="Topic 'topic name' doesn't exist"):
        msg_brk.subscribe("topic name")
    assert len(msg_brk._subscribed) == 0
    assert len(msg_brk._topics) == 0
    assert not msg_brk._topics


def test_subscribe_to_topic_twice():
    msg_brk = MessageBroker()
    topic_name = msg_brk.create_topic("topic name")
    topic_name = msg_brk.subscribe(topic_name)
    topic_name = msg_brk.subscribe(topic_name)

    assert topic_name == "topic name"
    assert "topic name" in msg_brk._subscribed and len(msg_brk._subscribed) == 1
    assert "topic name" in msg_brk._topics and len(msg_brk._topics) == 1


def test_consume_from_topic():
    msg_brk = MessageBroker()
    topic_name = msg_brk.create_topic("topic name")
    msg_brk.publish(topic_name, "message to publish 1")
    msg_brk.subscribe(topic_name)
    msg1 = msg_brk.consume(topic_name)
    msg_brk.publish(topic_name, "message to publish 2")
    msg2 = msg_brk.consume(topic_name)

    assert msg1 == "message to publish 1"
    assert msg2 == "message to publish 2"
    assert "topic name" in msg_brk._subscribed and len(msg_brk._subscribed) == 1
    assert "topic name" in msg_brk._topics and len(msg_brk._topics) == 1
    assert msg_brk._topics[topic_name].empty()


def test_consume_from_empty_topic():
    msg_brk = MessageBroker()
    topic_name = msg_brk.create_topic("topic name")
    msg_brk.subscribe(topic_name)
    msg1 = msg_brk.consume(topic_name)

    assert msg1 == None
    assert "topic name" in msg_brk._subscribed and len(msg_brk._subscribed) == 1
    assert "topic name" in msg_brk._topics and len(msg_brk._topics) == 1
    assert msg_brk._topics[topic_name].empty()


def test_consume_from_unknown_topic():
    msg_brk = MessageBroker()

    with pytest.raises(Exception, match="Cannot consume messages from topic if not subscribed"):
        msg_brk.consume("topic name")
    assert len(msg_brk._subscribed) == 0
    assert len(msg_brk._topics) == 0
    assert not msg_brk._topics
