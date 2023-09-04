from confluent_kafka import Producer

# Kafka broker settings
kafka_broker = 'kafkaserver:9092'
kafka_topic = 'log_topic'

# Create a Kafka producer instance
producer = Producer({'bootstrap.servers': kafka_broker})

# Read build logs from a file or another source (replace with your logic)
build_logs = "Build step 1...\nBuild step 2...\nBuild completed."

# Produce the build logs to the Kafka topic
producer.produce(kafka_topic, key='build_log_key', value=build_logs.encode('utf-8'))

# Wait for any outstanding messages to be delivered and delivery reports received
producer.flush()

print('Build logs sent to Kafka.')
