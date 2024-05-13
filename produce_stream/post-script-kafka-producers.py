#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr  8 12:00:04 2024

@author: kyrastyl
"""

from kafka import KafkaProducer
import json
import threading
import time, random
from datetime import datetime, timedelta
import sys

latest_ts_fitbit = None
latest_ts_locs = None
latest_ts_scale = None

numberOfEvents = 0
terminate = False

def create_producer(server='localhost:9092'):
    """Create a Kafka producer."""
    producer = KafkaProducer(bootstrap_servers=[server],
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    return producer

def send_message(producer, topic, send_interval):
    """Send a message to a Kafka topic at a specified interval."""
    while True:
        global numberOfEvents
        numberOfEvents +=1
        if numberOfEvents > 1000:
            global terminate
            terminate = True
        message = define_message(topic, send_interval)
        producer.send(topic, value=message)
        producer.flush()
        print(f"Message sent to topic {topic}")
        send_interval += random.uniform(0, 1)*send_interval
        time.sleep(send_interval)

def start_thread(producer, topic, send_interval):
    """Start a new thread for sending messages."""
    thread = threading.Thread(target=send_message, args=(producer, topic, send_interval))
    thread.daemon = True  # Daemon threads will automatically close when the main program exits
    thread.start()
    
def define_message(topic, send_interval):
    if topic == "Fitbit":
        return create_fitbit_message(send_interval)
    elif topic == "Scale":
        return create_scale_message(send_interval)
    elif topic == "Locations":
        return create_locs_message(send_interval)
    elif topic == "Terminate":
        return create_terminate_message()
    
def create_fitbit_message(send_rate):
    
    timestamp = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    steps = random.randint(100, 20000)*1.0
    stairs = random.randint(1, 15)*1.0
    naps = random.randint(1, 5)*1.0
    message = {"fitbit":{"timestamp":timestamp,"steps":steps,"stairs":stairs,"naps":naps}}
    
    print (message)
    
    return message

def create_scale_message(send_rate):
    
    timestamp = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    weight = random.uniform(68.0, 77.0)
    height = 175.0
    bmi = random.uniform(22.2, 22.9)
    bmiCat = "overweight"
    message = {"scale":{"timestamp":timestamp,"weight":weight,"height":height,"bmi":bmi,"bmiCategory":bmiCat}}
    
    print(message)
    
    return message

def create_locs_message(send_rate):
    
    timestamp = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    loc = "Bedroom"
    dateEntered = (datetime.now() - timedelta(seconds=send_rate/2)).strftime('%Y-%m-%dT%H:%M:%S')
    dateUpdate = timestamp
    totalActivations = random.randint(1, 10)
    recentActivations = random.randint(1, totalActivations)
    message = {"location":loc,"timestamp":timestamp, "dateEntered":dateEntered, "dateUpdate":dateUpdate, "totalActivations": totalActivations, "recentActivations":recentActivations}
    
    print(message)
    
    
    return message

def create_terminate_message():
    return {"message":"terminate process"}
    
    

if __name__ == '__main__':
    
    latest_ts_fitbit, latest_ts_locs, latest_ts_scale = datetime.now(), datetime.now(), datetime.now()
    
    # Create the Kafka producer
    producer = create_producer()

    # Define topics, messages, and their corresponding send intervals (in seconds)
    topics_info = [
                    ('Fitbit', 10),  # Send every 20 seconds
                    ('Scale', 60), # Send every 240 seconds (4min)
                    ('Locations', 30) # Send every 60 seconds (1min)
                ]

    # Start a new thread for each topic
    for topic, interval in topics_info:
        start_thread(producer, topic, interval)

    # Keep the main thread alive to allow daemon threads to run
    try:
        while True:
            time.sleep(1)
            if terminate:
                send_message(producer, "Terminate", 0)
                sys.exit(3)
    except KeyboardInterrupt:
        sys.exit(2)
        print("Stopped by the user")
