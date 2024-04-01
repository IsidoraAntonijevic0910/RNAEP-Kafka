from kafka import KafkaConsumer, KafkaProducer
import json
import requests


consumer = KafkaConsumer('topic1', bootstrap_servers='localhost:9092', group_id='my-group')
producer = KafkaProducer(bootstrap_servers='localhost:9092', acks='all')

nodejs_server_url = 'http://localhost:8000/api/receive-data'


for message in consumer:
    
    weather_data = message.value.decode('utf-8')
    weather_data_json = json.loads(weather_data)
    
    print("Received weather data:", weather_data_json)
    
    response = requests.post(nodejs_server_url, json=weather_data_json)
    
    print(f"Response from Node.js server: {response.text}")
    
    producer.send('topic2', value=message.value)


consumer.close()
producer.close()
