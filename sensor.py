import json
import random
import string
import time
import uuid
import sys
import boto3

client = boto3.client(
    'kinesis',
    aws_access_key_id='XXXXXX',
    aws_secret_access_key='XXXXXXXXXXX',
    region_name='us-west-1',
)
sensor_id = sys.argv[1]
print(f'Sensor de tempetura id: ${sensor_id}')
for _ in range(100000):
    data = {
        'id': sensor_id,
        'temperatura': random.randint(60, 120),
        'random_string': ''.join(random.choice(string.ascii_lowercase) for i in range(10))
    }
    response = client.put_record(
        StreamName='fabrica-acme',
        Data=json.dumps(data),
        PartitionKey="partitionkey")
    print(data)
    time.sleep(0.3)

