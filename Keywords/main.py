# this code recieves data with a column called 'extracted_keywords'
#
# "extracted_keywords": [
#     "[('data engineer', 0.6018), ('software engineering skills', 0.5121), ('software engineer', 0.4633), ('software engineering principles', 0.2394), ('someone', 0.1321)]"
# ]
#
# The output is the sum of the values for the extracted_keywords
#



from quixstreams import Application, State
from quixstreams.models.serializers.quix import QuixDeserializer, QuixTimeseriesSerializer, JSONSerializer
import os
import time
import ast


app = Application.Quix("keywords-2", auto_offset_reset="earliest")
input_topic = app.topic(os.environ["input"], value_deserializer=QuixDeserializer())
output_topic = app.topic(os.environ["output"], value_serializer=JSONSerializer())


def process_rows(row: dict, state: State):

    # Convert the 'extracted_keywords' field from a string to a list of tuples
    if 'extracted_keywords' not in row or row['extracted_keywords'] is None:
        print(f"Warning: row does not have an 'extracted_keywords' field or it's None: {row}")
        return

    print("---------------")
    print("extracted_keywords" in row)
    print("---------------")

    new_rows = dict(ast.literal_eval(row['extracted_keywords']))
    new_rows['Timestamp'] = row['Timestamp']
    
    print("---------------")
    print("1")
    print(new_rows)
    print("---------------")
    
    # remove columns 
    if 'parent_id' in new_rows:
        del new_rows['parent_id']
    if 'author' in new_rows:
        del new_rows['author']
    if 'body' in new_rows:
        del new_rows['body']
    if 'human_timestamp' in new_rows:
        del new_rows['human_timestamp']
    
    print("---------------")
    print("2")
    print(new_rows)
    print("---------------")
    
    # get sums from state, init with empty {} if not there
    sums_state = state.get("sums", {})
    
    print("---------------")
    print("3")
    print("---------------")
    
    # get/add to/from state and sum the values
    for key in new_rows:
        if key not in sums_state:
            sums_state[key] = new_rows[key]
        else:
            sums_state[key] += new_rows[key]

        new_rows[key] = sums_state[key]
    
    print("---------------")
    print("4")
    print("---------------")
    
    print("---------------")
    print("---------------")
    print(sums_state)
    print("---------------")
    print("---------------")

    # update state with the new state
    state.set('sums', sums_state)
    print("---------------")
    print("5")
    print("---------------")
    
    # return the new rows, which will update the sdf 
    # (this function was called with apply)
    return new_rows


# expand keywords from a nested dict to rows (keeping the timestamp)
def expand_keywords(row: dict):
    new_rows = row['extracted_keywords']

    # we need the timestamp, otherwise we could use sdf's expand function
    new_rows['Timestamp'] = row['Timestamp']

    return new_rows


def sum_keywords(row: dict, state: State):
    sums_state = state.get("sums", {})
    for key in row:
        if key not in sums_state:
            sums_state[key] = row[key]
        else:
            sums_state[key] += row[key]

        row[key] = sums_state[key]
    
    state.set('sums', sums_state)

def sdf_way():
    sdf = app.dataframe(input_topic)

    # filter data
    sdf = sdf[sdf.contains('extracted_keywords')]
    sdf = sdf[sdf['extracted_keywords'].notnull()]

    # parse extracted keyword column (change string to dict)

    sdf = sdf.update(lambda row: print(sdf.contains('extracted_keywords')))

    sdf['extracted_keywords'] = sdf['extracted_keywords'].apply(lambda value: dict(ast.literal_eval(value)))
    # sdf = sdf.update(lambda row: print(row))

    # expand keywords from a nested dict to rows (keeping the timestamp)
    sdf = sdf.apply(expand_keywords)

    # sum keywords and save to state
    sdf = sdf.update(sum_keywords, stateful=True)

    # print
    # sdf = sdf.update(lambda row: print(row))

    # publish to output topic
    sdf = sdf.to_topic(output_topic)
    return sdf

def old_way():
    sdf = app.dataframe(input_topic)

    # do all the processing in process_rows
    sdf = sdf.apply(process_rows, stateful=True)

    # print
    sdf = sdf.update(lambda row: print(row))

    # publish to output topic
    #sdf = sdf.to_topic(output_topic)
    return sdf


# uncomment the one you want to use..
sdf = sdf_way()
#sdf = old_way()

if __name__ == "__main__":
    app.run(sdf)
