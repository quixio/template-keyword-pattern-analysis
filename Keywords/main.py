from quixstreams import Application, State
from quixstreams.models.serializers.quix import QuixDeserializer, QuixTimeseriesSerializer
import os
import time
import ast


app = Application.Quix("keywords-2", auto_offset_reset="earliest")
input_topic = app.topic(os.environ["input"], value_deserializer=QuixDeserializer())
output_topic = app.topic(os.environ["output"], value_serializer=QuixTimeseriesSerializer())

sdf = app.dataframe(input_topic)
#output_sdf = app.dataframe(output_topic)

# Initialize an empty dictionary to store the counts and total scores
keyword_data = {}

# def func(d: dict):
#     print(d)

# #sdf = (
# app.dataframe(input_topic)
# # using a dummy function here assuming the incoming value is already a list
# sdf = sdf.apply(lambda value: func)
# # each item from the list will be produced to the output topic as a separate message
# sdf = sdf.to_topic(output_topic)
# #)

# if __name__ == "__main__":
#     app.run(sdf)

def reply(row: dict):
    global keyword_data

    # Convert the 'extracted_keywords' field from a string to a list of tuples
    if 'extracted_keywords' not in row or row['extracted_keywords'] is None:
        print(f"Warning: row does not have an 'extracted_keywords' field or it's None: {row}")
        return

    data = ast.literal_eval(row['extracted_keywords'])

    print("---")
    #print(data)
    print("---")

    # Process the data
    for keyword, score in data:
        if keyword not in keyword_data:
            # If the keyword is not in the dictionary, add it with the current count and score
            print(f"Adding kw {keyword}")
            keyword_data[keyword] = {'count': 1, 'total_score': score}
        else:
            # If the keyword is already in the dictionary, increment the count and add to the total score
            print(f"incrementing kw {keyword}")

            keyword_data[keyword]['count'] += 1
            keyword_data[keyword]['total_score'] += score

    # Print the results
    for keyword, data in keyword_data.items():
        print(f"Keyword: {keyword}, Count: {data['count']}, Total Score: {data['total_score']}")
        row["abc"] = "hi"

    
    return row        

# def reply(row: dict):
#     print(row)


sdf = sdf[sdf.contains('extracted_keywords')]
sdf = sdf[sdf['extracted_keywords'].notnull()]

sdf = sdf.update(lambda row: print(row))
#sdf = sdf.apply(lambda value: value['extracted_keywords'], expand=True)
#sdf = sdf.update(lambda row: print(row))



#sdf = sdf.apply(reply, expand=True)
#sdf = sdf[sdf.apply(lambda row: row is not None)]
#sdf["Timestamp"] = sdf["Timestamp"].apply(lambda row: time.time_ns())
# sdf["total"] = sdf.update(lambda r: 10)
#sdf = sdf.to_topic(output_topic)


if __name__ == "__main__":
    app.run(sdf)
