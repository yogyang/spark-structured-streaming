# !/usr/bin/env python
# -*- coding: utf-8 -*-
"""Docstring for module."""


from datetime import datetime, timedelta
from time import sleep

from pykafka import KafkaClient

bootstrap_servers = 'localhost:9092'
topic = b'test.1'

datetimeformat = '%Y-%m-%dT%H:%M:%S'


def pykafka_producer(use_rdkafka=False):
    """Example pykafka-producer to be used."""
    client = KafkaClient(hosts=bootstrap_servers)
    topic_to_write = client.topics[topic]
    start_date = datetime.now()
    count = 1
    with topic_to_write.get_producer(use_rdkafka=use_rdkafka) as producer:
        for i in range(1, count + 1):
            tstamp = datetime.now()
            msg_payload = "user_id:{0};{1};event_number_{2}".format(2, tstamp, i)
            sleep(1)
            print(msg_payload)
            producer.produce(msg_payload.encode())
    print("Successfully dumped data to kafka-topic: {0}".format(topic))


if __name__ == '__main__':
    pykafka_producer()
