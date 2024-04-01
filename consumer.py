from kafka import KafkaConsumer, KafkaProducer
import json

# Initialize Kafka consumer
consumer = KafkaConsumer('topic1', bootstrap_servers='localhost:9092', group_id='my-group')
# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092', acks='all')

# Read and process messages from the Kafka topic
for message in consumer:
    # Decode and load JSON data from the Kafka message
    weather_data = message.value.decode('utf-8')
    weather_data_json = json.loads(weather_data)
    
    # Process weather data (example: here you can manipulate the data as needed)
    processed_weather_data = weather_data_json
    
    # Send processed data to another Kafka topic
    producer.send('topic2', value=json.dumps(processed_weather_data).encode('utf-8'))
    
    # Print the processed data (optional)
    print("Processed weather data:", processed_weather_data)

# Close the consumer and producer (this part will not be reached in this example)
consumer.close()
producer.close()
