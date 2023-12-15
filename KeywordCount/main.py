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
    # Initialize state if it doesn't exist
    if not state.exists("counts"):
        state["counts"] = {
            "1min": {},
            "15min": {},
            "60min": {}
        }

    # Get current timestamp
    current_timestamp = datetime.fromtimestamp(row['Timestamp'] / 1e9)

    # Update counts
    for keyword, count in row.items():
        if keyword != 'Timestamp':
            for window, counts in state["counts"].items():
                # Calculate window start time
                if window == "1min":
                    window_start = current_timestamp - timedelta(minutes=1)
                elif window == "15min":
                    window_start = current_timestamp - timedelta(minutes=15)
                elif window == "60min":
                    window_start = current_timestamp - timedelta(minutes=60)

                # Remove counts outside of window
                keys_to_remove = []
                for timestamp, keyword_counts in counts.items():
                    if timestamp < window_start:
                        keys_to_remove.append(timestamp)

                for key in keys_to_remove:
                    del counts[key]

                # Add new count
                if current_timestamp not in counts:
                    counts[current_timestamp] = {}
                if keyword not in counts[current_timestamp]:
                    counts[current_timestamp][keyword] = 0
                counts[current_timestamp][keyword] += count

    print(state["counts"])  # Debug print
    return state["counts"]

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