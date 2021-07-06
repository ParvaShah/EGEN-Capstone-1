import os
from google.cloud import storage
import requests
import json
from google.cloud import pubsub_v1
from concurrent.futures import TimeoutError
from datetime import datetime
import time
from publisher import Publisher


os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="keys_pubsub12.json"
# GCP topic, project & subscription ids
PUB_SUB_TOPIC = "egen-pubsub-test"
PUB_SUB_PROJECT = "innate-mix-317300"
PUB_SUB_SUBSCRIPTION = "egen-pubsub-test-sub"
timeout = 3.0
storage_client = storage.Client()
bucket = storage_client.get_bucket("test_egen_pubsub_parva")


for i in range(5):    
    print("===================================",i)
    pubVar = Publisher()
    myResponse = requests.get("https://api.polygon.io/v2/last/trade/AAPL?&apiKey=bifpd9GaS56M0T0abMie9AA6ppkg4zK8")
    if 200 <= myResponse < 400:

        payload = {"data" : json.loads(myResponse.content), "timestamp": datetime.strftime(datetime.now(),"%Y-%m-%d %H:%M:%S")}
        print(f"Sending payload: {payload}.")
        pubVar.push_payload(payload, PUB_SUB_TOPIC, PUB_SUB_PROJECT)
        time.sleep(10)
    else:
        raise Exception("Error in loading data")
   