from quixstreams import Application
from quixstreams.models.serializers.quix import QuixDeserializer, JSONDeserializer

import os
import redis

r = redis.Redis(
    host=os.environ['redis_host'],
    port=int(int(os.environ['redis_port'])),
    password=os.environ['redis_password'],
    username=os.environ['redis_username'] if 'redis_username' in os.environ else None,
    decode_responses=True)

storage_key = os.environ['storage_key']

app = Application.Quix(consumer_group="redis-destination")

input_topic = app.topic(os.environ["input"], value_deserializer=JSONDeserializer())


def send_data_to_redis(value: dict) -> None:
    print(value)

    pipe = r.pipeline()
    pipe.ts().add(key=storage_key, value=value)
    pipe.execute()


sdf = app.dataframe(input_topic)
sdf = sdf.update(send_data_to_redis)

if __name__ == "__main__":
    print("Starting application")
    app.run(sdf)