#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr 11 00:50:56 2024

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


def create_producer(server='localhost:9092'):
    """Create a Kafka producer."""
    producer = KafkaProducer(bootstrap_servers=[server],
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    return producer

def send_message(producer, topic, message, send_interval):
    """Send a message to a Kafka topic at a specified interval."""
    print(message)
    producer.send(topic, value=message)
    producer.flush()
    print(f"Message sent to topic {topic}")
    print(" ========================  ")
    time.sleep(send_interval)
    
def create_fitbit_message(timestamp, steps, stairs, naps, send_rate):
    return {"fitbit":{"timestamp":timestamp,"steps":steps,"stairs":stairs,"naps":naps}}
    

def create_scale_message(timestamp, weight, height, bmi, bmiCat, send_rate):
    return {"scale":{"timestamp":timestamp,"weight":weight,"height":height,"bmi":bmi,"bmiCategory":bmiCat}}


def create_locs_message(timestamp, loc, dateEntered, dateUpdate, totalActivations, recentActivations, send_rate):
    return {"location":loc,"timestamp":timestamp, "dateEntered":dateEntered, "dateUpdate":dateUpdate, "totalActivations": totalActivations, "recentActivations":recentActivations}
    
def create_terminate_message():
    return {"message":"terminate process"}
    

if __name__ == '__main__':
    
    producer = create_producer()
    
    fitbit1 = create_fitbit_message(datetime.now().strftime('%Y-%m-%dT%H:%M:%S'), 5000, 3.0, 2.0, 60)
    send_message(producer, "Fitbit", fitbit1, 60)
    
    fitbit2 = create_fitbit_message(datetime.now().strftime('%Y-%m-%dT%H:%M:%S'), 5050, 5.0, 2.0, 60)
    time.sleep(60)
    
    locs1 = create_locs_message(datetime.now().strftime('%Y-%m-%dT%H:%M:%S'), "Bedroom", "2024-01-01T10:10:00", "2024-01-01T10:15:00", 5.0, 3.0, 60)
    send_message(producer, "Locations", locs1, 60)
    
    scale1 = create_scale_message(datetime.now().strftime('%Y-%m-%dT%H:%M:%S'), 85.0, 175, 22.5, "normal", 60)
    send_message(producer, "Scale", scale1, 60)
    
    send_message(producer, "Fitbit", fitbit2, 60)

    
    fttest = create_fitbit_message(datetime.now().strftime('%Y-%m-%dT%H:%M:%S'), 1, 1.0, 1.0, 10)
    send_message(producer, "Fitbit", fttest, 10)
    
    
    fitbit3 = create_fitbit_message(datetime.now().strftime('%Y-%m-%dT%H:%M:%S'), 5000, 3.0, 2.0, 60)
    send_message(producer, "Fitbit", fitbit3, 60)
    
    fitbit4 = create_fitbit_message(datetime.now().strftime('%Y-%m-%dT%H:%M:%S'), 5050, 5.0, 2.0, 60)
    send_message(producer, "Fitbit", fitbit4, 60)
    
    locs2 = create_locs_message(datetime.now().strftime('%Y-%m-%dT%H:%M:%S'), "Bedroom", "2024-01-01T11:20:00", "2024-01-01T11:25:00", 5.0, 3.0, 60)
    send_message(producer, "Locations", locs2, 60)
    
    scale2 = create_scale_message(datetime.now().strftime('%Y-%m-%dT%H:%M:%S'), 85.0, 175, 22.5, "normal", 60)
    send_message(producer, "Scale", scale2, 60)
    
    terminate = create_terminate_message()
    send_message(producer, "Terminate", terminate, 60)
    
    producer.close()
    
    

    # Define topics, messages, and their corresponding send intervals (in seconds)
    topics_info = [
                    ('Fitbit', 60),  # Send every 20 seconds
                    ('Scale', 5*60), # Send every 300 seconds (5min)
                    ('Locations', 2*60), # Send every 120 seconds (2min)
                    ('Terminate', 10)
                ]
