import quixstreams as qx
import time
import datetime
import math
import os
import random


# Quix injects credentials automatically to the client. 
# Alternatively, you can always pass an SDK token manually as an argument.
client = qx.QuixStreamingClient()

# Open the output topic where to write data out
topic_producer = client.get_topic_producer(topic_id_or_name = os.environ["output"])

# Set stream ID or leave parameters empty to get stream ID generated.
stream = topic_producer.create_stream()
stream.properties.name = "Hello World Python stream"

# Add metadata about time series data you are about to send. 
stream.timeseries.add_definition("ParameterA").set_range(-1.2, 1.2)
stream.timeseries.buffer.time_span_in_milliseconds = 100

print("Sending values for 30 seconds.")

# "extracted_keywords": [
#       "[('dashboard latency', 0.6075), ('data', 0.328), ('amplitude tech blog', 0.2962), ('precalculation', 0.287), ('duration', 0.2192)]"
#     ],

for index in range(0, 3000 * 10000):

    # List of 10 different words
    words = ['database', 'oreo', 'python', 'java', 'javascript', 'html', 'css', 'sql', 'ruby', 'php']

    # Generate 1 to 4 items
    num_items = random.randint(1, 4)

    # Generate the string
    result = []
    for _ in range(num_items):
        word = random.choice(words)
        score = round(random.uniform(0, 1), 3)
        result.append((word, score))

    # Convert the list to a string
    result_str = str(result)

    stream.timeseries \
        .buffer \
        .add_timestamp(datetime.datetime.utcnow()) \
        .add_value("extracted_keywords", result_str) \
        .publish()
    time.sleep(0.5)

print("Closing stream")
stream.close()