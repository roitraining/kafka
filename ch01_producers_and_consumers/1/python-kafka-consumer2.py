from kafka import KafkaConsumer
consumer = KafkaConsumer('stocks2')
print("consumer = ", consumer)
for event in consumer:
   print(event)
