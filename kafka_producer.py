import json, time, random
from datetime import datetime
from kafka import KafkaProducer

# Connect to Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# MovieLens 1M has users 1-6040 and movies 1-3952
USER_IDS = list(range(1, 6041))
MOVIE_IDS = list(range(1, 3953))

# Simulate trending — some movies get higher traffic
TRENDING_MOVIES = [318, 296, 2571, 1, 50, 858, 260, 2858, 593, 1196]

print("Kafka Producer started — sending events to topic: movie-ratings")
print("Press Ctrl+C to stop\n")

count = 0
try:
    while True:
        # 30% chance event is about a trending movie (simulates spikes)
        if random.random() < 0.3:
            movie_id = random.choice(TRENDING_MOVIES)
            rating = round(random.uniform(3.5, 5.0), 1)
        else:
            movie_id = random.choice(MOVIE_IDS)
            rating = round(random.uniform(1.0, 5.0), 1)

        event = {
            "user_id": random.choice(USER_IDS),
            "item_id": movie_id,
            "rating": rating,
            "timestamp": datetime.now().isoformat()
        }

        producer.send('movie-ratings', value=event)
        count += 1

        if count % 10 == 0:
            print(f"Sent {count} events | Latest: user={event['user_id']} movie={event['item_id']} rating={event['rating']}")

        # Send ~20 events per second
        time.sleep(0.05)

except KeyboardInterrupt:
    print(f"\nStopped. Total events sent: {count}")
    producer.flush()
    producer.close()

