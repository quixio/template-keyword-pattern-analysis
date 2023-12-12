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

    new_rows = dict(ast.literal_eval(row['extracted_keywords']))
    new_rows['Timestamp'] = row['Timestamp']
    if 'parent_id' in new_rows:
        del new_rows['parent_id']
    if 'author' in new_rows:
        del new_rows['author']
    if 'body' in new_rows:
        del new_rows['body']
    if 'human_timestamp' in new_rows:
        del new_rows['human_timestamp']

    sums_state = state.get("sums", {})
    for key in new_rows:
        if key not in sums_state:
            sums_state[key] = new_rows[key]
        else:
            sums_state[key] += new_rows[key]

        new_rows[key] = sums_state[key]
    
    state.set('sums', sums_state)

    return new_rows
       

# def reply(row: dict):
#     print(row)


def expand_keywords(row: dict):
    new_rows = row['extracted_keywords']
    #print(new_rows)
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
    sdf = sdf[sdf.contains('extracted_keywords')]
    sdf = sdf[sdf['extracted_keywords'].notnull()]
    sdf['extracted_keywords'] = sdf['extracted_keywords'].apply(lambda value: dict(ast.literal_eval(value)))
    sdf = sdf.apply(expand_keywords)
    sdf = sdf.update(sum_keywords, stateful=True)
    sdf = sdf.update(lambda row: print(row))
    sdf = sdf.to_topic(output_topic)
    return sdf

def old_way():
    sdf = app.dataframe(input_topic)
    sdf = sdf.apply(process_rows, stateful=True)
    sdf = sdf.update(lambda row: print(row))
    sdf = sdf.to_topic(output_topic)
    return sdf



#sdf = sdf_way()
sdf = old_way()

if __name__ == "__main__":
    app.run(sdf)
