#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May 15 04:05:56 2024

@author: kyrastyl
"""

import json
import time
import argparse
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic

def create_producer(server='localhost:9092'):
    """Create a Kafka producer."""
    producer = KafkaProducer(bootstrap_servers=[server],
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    return producer

def create_topics(admin_client, topics):
    """Create Kafka topics if they do not exist."""
    existing_topics = admin_client.list_topics()
    topics_to_create = [NewTopic(name=topic, num_partitions=1, replication_factor=1) for topic in topics if topic not in existing_topics]

    if topics_to_create:
        admin_client.create_topics(new_topics=topics_to_create, validate_only=False)

def send_message(producer, topic, message, send_interval):
    """Send a message to a Kafka topic at a specified interval."""
    print(f"Sending message to topic {topic}: {message}")
    producer.send(topic, value=message)
    producer.flush()
    print(f"Message sent to topic {topic}")
    print(" ========================  ")
    time.sleep(send_interval)

def read_messages(file_path):
    """Read messages from a file."""
    with open(file_path, 'r') as file:
        messages = [json.loads(line.strip()) for line in file]
    return messages

def main():
    parser = argparse.ArgumentParser(description="Script to send messages from a file to Kafka topics.")
    
    # Required arguments
    required_args = parser.add_argument_group('required arguments')
    required_args.add_argument('-f', '--file', type=str, required=True, help="Path to the input file")
    required_args.add_argument('-s', '--server', type=str, required=False, help="Kafka server address")
    
    # Optional arguments
    parser.add_argument('--interval', type=int, default=1, help="Interval between sending messages in seconds")
    
    args = parser.parse_args()

    print(f"Connecting to Kafka server: {args.server}")

    topics = ["Fitbit", "Scale", "Locations"]

    # Create Kafka admin client to manage topics
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=args.server)
        create_topics(admin_client, topics)
        admin_client.close()
    except Exception as e:
        print(f"Failed to create Kafka admin client or topics: {e}")
        return

    try:
        producer = create_producer(server=args.server)
    except Exception as e:
        print(f"Failed to create Kafka producer: {e}")
        return

    messages = read_messages(args.file)

    for message in messages:
        topic = "Terminate"
        if 'fitbit' in message:
            topic = "Fitbit"
        elif 'scale' in message:
            topic = "Scale"
        elif 'location' in message:
            topic = "Locations"
        send_message(producer, topic, message, args.interval)

    producer.close()

if __name__ == '__main__':
    main()
