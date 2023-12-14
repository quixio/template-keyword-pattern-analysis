from quixstreams import Application, State
from quixstreams.models.serializers.quix import QuixDeserializer, QuixTimeseriesSerializer, JSONSerializer
import os
import time
import ast
from datetime import datetime, timedelta


app = Application.Quix("keywords-3", auto_offset_reset="earliest")
input_topic = app.topic(os.environ["input"], value_deserializer=QuixDeserializer())
output_topic = app.topic(os.environ["output"], value_serializer=JSONSerializer())


# expand keywords from a nested dict to rows (keeping the timestamp)
def expand_keywords(row: dict):
    new_rows = row['extracted_keywords']

    # we need the timestamp, otherwise we could use sdf's expand function
    new_rows['Timestamp'] = row['Timestamp']
    return new_rows

clear_state = True
def sum_keywords(row: dict, state: State):
    global clear_state

    print("--")
    print(row)
    print("--")

    if clear_state:
        print("Clearing state")

        state.set("timestamps", {})
        clear_state = False

    timestamps = state.get("timestamps", {})

    current_time = datetime.fromtimestamp(row['Timestamp'] / 1e9).isoformat()

    for key in row:
        print("--")
        print(f"{key}")
        print("--")

        if key == "Timestamp":
            continue

        # Update counts for current time
        current_counts = timestamps.get(current_time, [])
        current_counts.append({key: current_counts.get(key, 0) + 1})
        timestamps[current_time] = current_counts

    # Delete counts older than 15 minutes
    for timestamp in list(timestamps.keys()):
        timestamp_datetime = datetime.fromisoformat(timestamp)
        if current_time - timestamp_datetime > timedelta(minutes=15):
            print(f"Deleting {timestamp}")
            del timestamps[timestamp]

    state.set('timestamps', timestamps)

    print("--")
    print(timestamps)
    print("--")

    return row

# def sum_keywords(row: dict, state: State):
#     global clear_state


#     print("-1-")
#     print(row)

#     if clear_state:
#         state.set("counts", {})
#         clear_state = False

#     sums_state = state.get("counts", {})
    
#     print("-2-")
#     print(sums_state)

#     for key in row:
#         print("-2a-")
#         print(key)

#         if key not in sums_state or key == "Timestamp":
#             print("-2b-")
#             sums_state[key] = 1#row[key]
#         else:
#             print("-2c-")
#             sums_state[key] += 1#row[key]
        
#         if key == "Timestamp":
#             sums_state[key] = row[key]

#         print("-2d-")
#         print(row[key])
#         row[key] = sums_state[key]
    
#     print("-3-")
#     print(sums_state)

#     state.set('counts', sums_state)
#     #time.sleep(0.3)

#     return row
#     #return sums_state


def sdf_way():
    sdf = app.dataframe(input_topic)

    # filter data
    sdf = sdf[sdf.contains('extracted_keywords')]
    sdf = sdf[sdf['extracted_keywords'].notnull()]

    #sdf = sdf.update(lambda row: print(row))

    # consider using....
    # Convert the string to a dictionary
    # my_dict = json.loads(value)

    sdf['extracted_keywords'] = sdf['extracted_keywords'].apply(lambda value: dict(ast.literal_eval(value)))
    # sdf = sdf.update(lambda row: print(row))

    # expand keywords from a nested dict to rows (keeping the timestamp)
    sdf = sdf.apply(expand_keywords)

    # sum keywords and save to state
    sdf = sdf.apply(sum_keywords, stateful=True)

    # print
    #print("====")
    #sdf = sdf.update(lambda row: print(f"&&&&&&&&&&&&&&&{row}&&&&&&&&&&&&&&&&"))
    #print("====")

    # publish to output topic
    sdf = sdf.to_topic(output_topic)
    return sdf

sdf = sdf_way()

if __name__ == "__main__":
    app.run(sdf)
