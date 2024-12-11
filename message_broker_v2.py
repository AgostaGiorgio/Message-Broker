from threading import Lock, Thread
from uuid import uuid4 as UUID
from time import sleep

class TopicRepository:
    _instance = None
    _lock = Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialize()
        return cls._instance

    def _initialize(self) -> None:
        self._topics = dict()
        self._subscribers = dict()
        self.lock = Lock()

    def create_topic(self, topic_name) -> str:
        with self.lock:
            if not self.topic_exists(topic_name):
                self._topics[topic_name] = []
        return topic_name
    
    def publish(self, topic_name, message) -> tuple[str, str]:
        with self.lock:
            if not self.topic_exists(topic_name):
                    raise Exception(f"Topic '{topic_name}' doesn't exist")
            self._topics[topic_name].append(message)
        return (topic_name, message)
    
    def subscribe(self, topic_name, id) -> int:
        with self.lock:
            if not self.topic_exists(topic_name):
                raise Exception(f"Topic '{topic_name}' doesn't exist")
            if not self.is_subscribed(topic_name, id):
                if self._subscribers.get(topic_name, None) is None:
                    self._subscribers[topic_name] = {}
                self._subscribers[topic_name][id] = 0
            return self._subscribers[topic_name][id]

    def consume(self, topic_name, id) -> str:
        with self.lock:
            if not self.topic_exists(topic_name):
                raise Exception(f"Topic '{topic_name}' doesn't exist")
            if not self.is_subscribed(topic_name, id):
                raise Exception("Cannot consume messages from topic if not subscribed")
            offset = self._subscribers[topic_name][id]
            if len(self._topics[topic_name]) <= offset:
                return None
        message = self._topics[topic_name][offset]
        self._subscribers[topic_name][id] += 1
        return message

    def topic_count(self):
        print(self._topics)
        return len(self._topics.values())
    
    def topic_exists(self, topic_name) -> bool:
        return topic_name in self._topics    
    
    def is_subscribed(self, topic_name, id) -> bool:
        return id in self._subscribers.get(topic_name, {})


class MessageBroker:
    '''
    Version 2 of MessageBroker class. In this version each instance of MessageBroker will 
    use a shared list of topics so multiple producer can publish in one topic and multiple consumer can
    consume messages from the same topic.
    '''

    def __init__(self):
        self.topic_repository = TopicRepository()
        self.id = UUID()

    def create_topic(self, topic_name) -> str:
        if topic_name is None:
            raise Exception("Topic name cannot be None")
        return self.topic_repository.create_topic(topic_name)

    def publish(self, topic_name, message):
        if message is None:
            raise Exception("Message cannot be None")
        self.topic_repository.publish(topic_name, message)

    def subscribe(self, topic_name) -> int:
        return self.topic_repository.subscribe(topic_name, self.id)

    def consume(self, topic_name) -> str:
        return self.topic_repository.consume(topic_name, self.id)


if __name__ == "__main__":
    broker = MessageBroker()
    broker2 = MessageBroker()

    broker.create_topic("orders")
    broker.publish("orders", "New order: #12345")
    broker.subscribe("orders")
    message = broker.consume("orders")
    print(message)  # Should print: "New order: #12345"


    broker2.subscribe("orders")
    message = broker2.consume("orders")
    broker2.publish("orders", "New order: #678910")
    print(message)  # Should print: "New order: #12345"

    message = broker2.consume("orders")
    print(message)  # Should print: "New order: #678910"
    message = broker.consume("orders")
    print(message)  # Should print: "New order: #678910"
    message = broker.consume("orders")
    print(message)  # Should print: "None"

    broker2.publish("orders", "New order saved")
    message = broker.consume("orders")
    print(message)  # Should print: "New order saved"
