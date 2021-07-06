import os
from google.cloud import storage
import requests
import json
from google.cloud import pubsub_v1
from concurrent.futures import TimeoutError
import time

class Publisher:
    # producer function to push a message to a topic
    def push_payload(self, payload, topic, project):        
        publisher = pubsub_v1.PublisherClient() 
        topic_path = publisher.topic_path(project, topic)        
        data = json.dumps(payload).encode("utf-8")           
        future = publisher.publish(topic_path, data=data)
        print("Pushed message to topic.") 

