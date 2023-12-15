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
    state_key = "counts_v3"  # State key variable

    # Initialize state if it doesn't exist
    counts = state.get(state_key, {
        "1min": {},
        "15min": {},
        "60min": {}
    })

    # Get current timestamp
    current_timestamp = datetime.fromtimestamp(row['Timestamp'] / 1e9)

    # Update counts
    for keyword, _ in row.items():
        if keyword != 'Timestamp':
            print(f"Processing keyword: {keyword}")  # Debug print
            for window, window_counts in counts.items():
                # Calculate window start time
                if window == "1min":
                    window_start = current_timestamp - timedelta(minutes=1)
                elif window == "15min":
                    window_start = current_timestamp - timedelta(minutes=15)
                elif window == "60min":
                    window_start = current_timestamp - timedelta(minutes=60)

                # Remove counts outside of window
                keys_to_remove = []
                for keyword in window_counts.keys():
                    keyword_counts = window_counts[keyword]
                    timestamps_to_remove = [ts for ts in keyword_counts if datetime.fromtimestamp(float(ts)) < window_start]
                    for ts in timestamps_to_remove:
                        del keyword_counts[ts]
                    if not keyword_counts:
                        keys_to_remove.append(keyword)

                for key in keys_to_remove:
                    del window_counts[key]

                # Add new count
                if keyword not in window_counts:
                    window_counts[keyword] = {}
                if str(current_timestamp.timestamp()) not in window_counts[keyword]:
                    window_counts[keyword][str(current_timestamp.timestamp())] = 0
                window_counts[keyword][str(current_timestamp.timestamp())] += 1

    # Debug print
    print({window: {keyword: sum(times.values()) for keyword, times in counts[window].items()} for window in counts}) 

    state.set(state_key, counts)
    return {window: {keyword: sum(times.values()) for keyword, times in counts[window].items()} for window in counts}
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