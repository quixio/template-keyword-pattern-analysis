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

        state.set("counts_1min", {})
        state.set("counts_15min", {})
        clear_state = False

    counts_1min = state.get("counts_1min", {})
    counts_15min = state.get("counts_15min", {})

    current_time = datetime.fromtimestamp(row['Timestamp'] / 1e9)

    for key in row:
        print("--")
        print(f"{key}")
        print("--")

        if key == "Timestamp":
            continue

        # Update counts for 1 minute
        count, _ = counts_1min.get(key, (0, current_time.isoformat()))
        counts_1min[key] = (count + 1, current_time.isoformat())
        for k, (count, timestamp) in list(counts_1min.items()):
            timestamp = datetime.fromisoformat(timestamp)
            if current_time - timestamp > timedelta(minutes=1):
                print(f"Deleting {k}")
                del counts_1min[k]

        # Update counts for 15 minutes
        count, _ = counts_15min.get(key, (0, current_time.isoformat()))
        counts_15min[key] = (count + 1, current_time.isoformat())
        for k, (count, timestamp) in list(counts_15min.items()):
            timestamp = datetime.fromisoformat(timestamp)
            if current_time - timestamp > timedelta(minutes=15):
                print(f"Deleting {k}")
                del counts_15min[k]

    state.set('counts_1min', counts_1min)
    state.set('counts_15min', counts_15min)

    print("--")
    print(counts_1min)
    print("--")
    print(counts_15min)
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
