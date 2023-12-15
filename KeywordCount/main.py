from quixstreams import Application, State
from quixstreams.models.serializers.quix import QuixDeserializer, QuixTimeseriesSerializer, JSONSerializer
import os
import time
import ast
from datetime import datetime, timedelta
from quixstreams.kafka import Producer


app = Application.Quix("keywords-3", auto_offset_reset="earliest")
input_topic = app.topic(os.environ["input"], value_deserializer=QuixDeserializer())
output_topic = app.topic(os.environ["output"], value_serializer=JSONSerializer())

def expand_keywords(row: dict):
    new_rows = row['extracted_keywords']
    new_rows['Timestamp'] = row['Timestamp']
    return new_rows

def sum_keywords(row: dict, state: State, some_param):
    print(row)
    return row

def sdf_way():
    sdf = app.dataframe(input_topic)
    sdf = sdf[sdf.contains('extracted_keywords')]
    sdf = sdf[sdf['extracted_keywords'].notnull()]
    sdf['extracted_keywords'] = sdf['extracted_keywords'].apply(lambda value: dict(ast.literal_eval(value)))
    sdf = sdf.apply(expand_keywords)
    #sdf = sdf.apply(sum_keywords, stateful=True)
    sdf = sdf.apply(lambda row, state: sum_keywords(row, state, "thing"), stateful=True)
    sdf = sdf.to_topic(output_topic)
    return sdf

sdf = sdf_way()

if __name__ == "__main__":
    app.run(sdf)