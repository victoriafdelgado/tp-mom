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
    
    def start_consuming(self, on_message_callback):
        try:
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
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(e)
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(e)

    def stop_consuming(self):
        try:
            self.channel.stop_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(e)


    def send(self, message):
        try:
            self.channel.basic_publish(exchange='',
                                    routing_key=self.queue_name,
                                    body=message,
                                    properties=pika.BasicProperties(
                                        delivery_mode = pika.DeliveryMode.Persistent
                                    ))
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(e)
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(e)


    def close(self):
        try:
            self.connection.close()
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareCloseError(e)


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        self.connection =  pika.BlockingConnection(pika.ConnectionParameters(host))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=exchange_name, exchange_type='direct')
        self.exchange_name = exchange_name
        self.result = self.channel.queue_declare(queue='', exclusive=True)
        self.queue_name = self.result.method.queue
        self.routing_keys = routing_keys
        for routing_key in self.routing_keys:
            self.channel.queue_bind(exchange=self.exchange_name, 
                                    queue=self.queue_name,
                                    routing_key=routing_key)
    
    def send(self, message):
        try:
            for routing_key in self.routing_keys:
                self.channel.basic_publish(exchange=self.exchange_name, 
                                        routing_key=routing_key, 
                                        body=message)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(e)
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(e)

    def close(self):
        try:
            self.connection.close()
        except pika.exceptions.AMQPError as e:
                raise MessageMiddlewareCloseError(e)

    def stop_consuming(self):
        try:
            self.channel.stop_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(e)
    

    def start_consuming(self, on_message_callback):
        try:
            def callback(ch, method, properties, body):
                    def ack():
                        ch.basic_ack(delivery_tag=method.delivery_tag)
                    def nack():
                        ch.basic_nack(delivery_tag=method.delivery_tag)
                    on_message_callback(body, ack, nack)

            self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback)
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(e)
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(e)
        
