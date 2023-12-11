from quixstreams import Application, State
from quixstreams.models.serializers.quix import QuixDeserializer, QuixTimeseriesSerializer
import os
import time

app = Application.Quix("keywords-1", auto_offset_reset="latest")
input_topic = app.topic(os.environ["input"], value_deserializer=QuixDeserializer())
output_topic = app.topic(os.environ["output"], value_serializer=QuixTimeseriesSerializer())

sdf = app.dataframe(input_topic)

# Initialize an empty dictionary to store the counts and total scores
keyword_data = {}

def reply(row: dict):

    # Check if the row has an 'extracted_keywords' field
    if 'extracted_keywords' not in row:
        print(f"Warning: row does not have an 'extracted_keywords' field: {row}")
        return

    data = row['extracted_keywords']
    print(data)

    # Check if data is a list of tuples
    if not isinstance(data, list) or not all(isinstance(item, tuple) and len(item) == 2 for item in data):
        print(f"Warning: 'extracted_keywords' field is not a list of tuples: {data}")
        return

    # Process the data
    for keyword, score in data:
        if keyword not in keyword_data:
            # If the keyword is not in the dictionary, add it with the current count and score
            keyword_data[keyword] = {'count': 1, 'total_score': score}
        else:
            # If the keyword is already in the dictionary, increment the count and add to the total score
            keyword_data[keyword]['count'] += 1
            keyword_data[keyword]['total_score'] += score

    # Print the results
    for keyword, data in keyword_data.items():
        print(f"Keyword: {keyword}, Count: {data['count']}, Total Score: {data['total_score']}")

sdf = sdf.apply(reply)

#sdf["Timestamp"] = sdf["Timestamp"].apply(lambda row: time.time_ns())

#sdf = sdf.to_topic(output_topic)

if __name__ == "__main__":
    app.run(sdf)