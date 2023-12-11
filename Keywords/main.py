from quixstreams import Application, State
from quixstreams.models.serializers.quix import QuixDeserializer, QuixTimeseriesSerializer
import os
import time

app = Application.Quix("keywords-1", auto_offset_reset="latest")
input_topic = app.topic(os.environ["input"], value_deserializer=QuixDeserializer())
output_topic = app.topic(os.environ["output"], value_serializer=QuixTimeseriesSerializer())

sdf = app.dataframe(input_topic)

def reply(row: dict):
    print(row['extracted_keywords'])

sdf = sdf.apply(reply)

#sdf["Timestamp"] = sdf["Timestamp"].apply(lambda row: time.time_ns())

#sdf = sdf.to_topic(output_topic)

if __name__ == "__main__":
    app.run(sdf)