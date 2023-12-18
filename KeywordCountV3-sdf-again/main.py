from quixstreams import Application, State
from quixstreams.models.serializers.quix import QuixDeserializer, QuixTimeseriesSerializer, JSONSerializer
import os
import time
import ast
from datetime import datetime, timedelta
from quixstreams.kafka import Producer
import json
from random import random, randint


window_length = int(os.getenv("window_length", 1))

app = Application.Quix(f"keywords-v6-{window_length}", auto_offset_reset="latest")
input_topic = app.topic(os.environ["input"], value_deserializer=QuixDeserializer())
output_topic = app.topic(os.environ["output"], value_serializer=JSONSerializer())

state_key = f"counts_tumbling_v18-{randint(1, 100000)}"  # State key variable


def sum_keywords_tumbling(row: dict, state: State, some_param):
    
    previous_window_start_state_key = state_key + "_previous_window_start"

    # Initialize state if it doesn't exist
    counts = state.get(state_key, {})
    ended_window = {}  # Store ended windows

     # Get current timestamp
    current_timestamp = datetime.fromtimestamp(row['Timestamp'] / 1e9)
    previous_window_start = state.get(previous_window_start_state_key, "") # if not set, default to current
    if previous_window_start == "":
        previous_window_start = current_timestamp.timestamp()
        #print(f"Setting {previous_window_start_state_key} state to {previous_window_start}")
        state.set(previous_window_start_state_key, previous_window_start)

    window_start = previous_window_start

    window_start_str = str(datetime.utcfromtimestamp(window_start))

    if window_start_str not in counts:
        #print(f"Adding window {w} to counts")
        counts[window_start_str] = {}

    window_counts = counts[window_start_str]
    
    # Update counts
    for keyword, _ in row.items():
        if keyword != 'Timestamp': #and "database" in keyword
            
            # Add new count
            if keyword not in window_counts:
                window_counts[keyword] = 0

            window_counts[keyword] = window_counts[keyword] + 1

            prev_start_dt = datetime.utcfromtimestamp(previous_window_start)

            
    print(f"WINDOW START: {prev_start_dt}")
    print(f"CURRNT TSTMP: {current_timestamp}")

    if current_timestamp > (prev_start_dt + timedelta(minutes=window_length)):
        print(f"Window ended at {current_timestamp}")

        #print("Current window data:")
        print(counts[window_start_str])
        ended_window = counts[window_start_str]

        counts[window_start_str] = {}
        counts.pop(window_start_str)
        
        previous_window_start = current_timestamp.timestamp()
        #print(f"Setting {previous_window_start_state_key} state to {previous_window_start}")
        state.set(previous_window_start_state_key, previous_window_start)
    else:
        ended_window = {}


    state.set(state_key, counts)
    print(f"returning: {ended_window}")
    return json.dumps(ended_window)  # Return ended window as JSON




def expand_keywords(row: dict):
    new_rows = row['extracted_keywords']
    new_rows['Timestamp'] = row['Timestamp']
    return new_rows


def sum_keywords(row: dict, state: State, some_param):
    state_key = "counts_v101"  # State key variable

    # Initialize state if it doesn't exist
    counts = state.get(state_key, {
        "1min": {},
        "15min": {},
        "60min": {},
        "4hr": {},
        "8hr": {},
        "24hr": {}
    })

    # Get current timestamp
    current_timestamp = datetime.fromtimestamp(row['Timestamp'] / 1e9)

    # Update counts
    for keyword, _ in row.items():
        if keyword != 'Timestamp':
            print(f"Processing keyword: {keyword}")  # Debug print
            for window in counts.keys():
                window_counts = counts[window]
                # Calculate window start time
                if window == "1min":
                    window_start = current_timestamp - timedelta(minutes=1)
                elif window == "15min":
                    window_start = current_timestamp - timedelta(minutes=15)
                elif window == "60min":
                    window_start = current_timestamp - timedelta(hours=1)
                elif window == "4hr":
                    window_start = current_timestamp - timedelta(hours=4)
                elif window == "8hr":
                    window_start = current_timestamp - timedelta(hours=8)
                elif window == "24hr":
                    window_start = current_timestamp - timedelta(hours=24)

                # Remove counts outside of window
                if keyword in window_counts:
                    timestamps_to_remove = [ts for ts in window_counts[keyword] if datetime.fromtimestamp(float(ts)) < window_start]
                    for ts in timestamps_to_remove:
                        del window_counts[keyword][ts]
                    if not window_counts[keyword]:
                        del window_counts[keyword]

                # Add new count
                if keyword not in window_counts:
                    window_counts[keyword] = {}
                if str(current_timestamp.timestamp()) not in window_counts[keyword]:
                    window_counts[keyword][str(current_timestamp.timestamp())] = 0
                window_counts[keyword][str(current_timestamp.timestamp())] += 1

                print(f"Updated counts for keyword {keyword} in window {window}: {window_counts[keyword]}")  # Debug print

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
    #sdf = sdf.apply(lambda row, state: sum_keywords(row, state, "thing"), stateful=True)
    sdf = sdf.apply(lambda row, state: sum_keywords_tumbling(row, state, "thing"), stateful=True)
    sdf = sdf.to_topic(output_topic)#key=str(f"1_min_window")
    return sdf

sdf = sdf_way()

if __name__ == "__main__":
    app.run(sdf)