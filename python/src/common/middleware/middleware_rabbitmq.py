import pika
import random
import string
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange, MessageMiddlewareCloseError, MessageMiddlewareDeleteError, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.connection =  pika.BlockingConnection(pika.ConnectionParameters(host))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=queue_name, durable=True, arguments={'x-queue-type': 'quorum'})
        self.queue_name = queue_name

    def send(self, message):
        self.channel.basic_publish(exchange='',
                                routing_key=self.queue_name,
                                body=message,
                                properties=pika.BasicProperties(
                                    delivery_mode = pika.DeliveryMode.Persistent
                                ))
        
    def close(self):
       self.connection.close()

    def stop_consuming(self):
        self.channel.stop_consuming()

    def start_consuming(self, on_message_callback):
        def callback(ch, method, properties, body):
           def ack():
               ch.basic_ack(delivery_tag=method.delivery_tag)
           def nack():
               ch.basic_nack(delivery_tag=method.delivery_tag)
           on_message_callback(body, ack, nack)

        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(queue=self.queue_name,
                                   on_message_callback=callback)
        self.channel.start_consuming()



class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        pass
