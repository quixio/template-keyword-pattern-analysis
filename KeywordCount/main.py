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

def time_delta_check(counts, key, state, current_time, window_start, delta_seconds):
    if current_time - window_start > timedelta(seconds=delta_seconds):
        return_data = counts[key]
        return_data["period"] = f"{delta_seconds} seconds"
        counts[key] = return_data


        print(f"Clearing state for {key}")
        counts[key] = {}
        state.set("counts", counts)
        state.set("window_start", current_time.isoformat())

        return counts

clear_state = True
def sum_keywords(row: dict, state: State):
    global clear_state

    if clear_state:
        print("Initializing state")

        state.set("counts", {"one_minute_data": {}, "15_minute_data": {}})
        state.set("window_start", datetime.fromtimestamp(row['Timestamp'] / 1e9).isoformat())
        clear_state = False

    counts = state.get("counts", {})
    window_start = datetime.fromisoformat(state.get("window_start"))

    current_time = datetime.fromtimestamp(row['Timestamp'] / 1e9)

    for key in row:
        if key == "Timestamp":
            continue

        for period in counts:
            print (period)
            if period not in counts:
                counts[period] = {}
            
            counts[period][key] = counts[period].get(key, 0) + 1

    state.set('counts', counts)

    #print("********************")
    #for p in counts:
    #    print(p)
    #print("********************")

    return_data = {}
    return_data = time_delta_check(counts, "one_minute_data", state, current_time, window_start, 60)
    #return_data = time_delta_check(counts, "15_minute_data", state, current_time, window_start, 900)



    return return_data

def sdf_way():
    sdf = app.dataframe(input_topic)
    sdf = sdf[sdf.contains('extracted_keywords')]
    sdf = sdf[sdf['extracted_keywords'].notnull()]
    sdf['extracted_keywords'] = sdf['extracted_keywords'].apply(lambda value: dict(ast.literal_eval(value)))
    sdf = sdf.apply(expand_keywords)
    sdf = sdf.apply(sum_keywords, stateful=True)
    sdf = sdf.to_topic(output_topic)
    return sdf

sdf = sdf_way()

if __name__ == "__main__":
    app.run(sdf)