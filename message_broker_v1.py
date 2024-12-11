import queue


class MessageBroker:
    '''
    Version 1 of MessageBroker class. In this first version each instance of MessageBroker
    will have his own list of topics so there will be always 1 producer and 1 consumer.
    '''

    def __init__(self):
        self._topics = dict()
        self._subscribed = set()

    def create_topic(self, topic_name) -> str:
        if topic_name is None:
            raise Exception("Topic name cannot be None")
        if topic_name not in self._topics:
            self._topics[topic_name] = queue.Queue()
        return topic_name

    def publish(self, topic_name, message):
        if message is None:
            raise Exception("Message cannot be None")
        if topic_name not in self._topics:
            raise Exception(f"Topic '{topic_name}' doesn't exist")
        self._topics[topic_name].put(message)

    def subscribe(self, topic_name) -> str:
        if topic_name not in self._topics:
            raise Exception(f"Topic '{topic_name}' doesn't exist")
        self._subscribed.add(topic_name)
        return topic_name

    def consume(self, topic_name) -> str:
        if topic_name not in self._subscribed:
            raise Exception("Cannot consume messages from topic if not subscribed")
        topic = self._topics[topic_name]
        if topic.empty():
            return None
        return topic.get(timeout=1)


if __name__ == "__main__":
    broker = MessageBroker()
    broker.create_topic("orders")
    broker.publish("orders", "New order: #12345")
    broker.subscribe("orders")
    message = broker.consume("orders")

    print(message)  # Should print: "New order: #12345"
