#!/usr/bin/env python
# coding: utf-8

# In[1]:


#publisher.py
import pika
class Publisher:
    def __init__(self, config):
        self.config = config
    def publish(self, routing_key, message):
        credentials = pika.PlainCredentials('guest', 'Pewdiepie1')
        parameters = pika.ConnectionParameters('localhost', credentials=credentials, heartbeat=5)
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        channel.exchange_declare(exchange=self.config['exchange'], exchange_type='topic')
        channel.basic_publish(exchange=self.config['exchange'],
        routing_key=routing_key, body=message)
        print(' [x] Sent message %r for %r' % (message,routing_key))
    def create_connection(self):
        param = pika.ConnectionParameters(host=self.config['host'], port=self.config['port']) 
        return pika.BlockingConnection(param)
config = { 'host': 'localhost','port': 5672, 'exchange' : 'my_exchange'}
publisher = Publisher(config)
publisher.publish('nse.nifty', 'New Data')


# In[3]:




