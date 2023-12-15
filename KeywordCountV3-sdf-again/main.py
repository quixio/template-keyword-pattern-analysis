import quixstreams as qx
import os
import pandas as pd
from collections import defaultdict
from datetime import datetime, timedelta
import ast
import signal
import traceback


client = qx.QuixStreamingClient()

topic_consumer = client.get_topic_consumer(os.environ["input"], consumer_group = "empty-transformation")
topic_producer = client.get_topic_producer(os.environ["output"])

# Initialize a dictionary to store keyword counts and timestamps
keyword_counts = defaultdict(lambda: defaultdict(list))

def on_dataframe_received_handler(stream_consumer: qx.StreamConsumer, df: pd.DataFrame):
    
    if 'extracted_keywords' not in df:
        print('extracted_keywords not found in dataframe')
    else:
        try:
            # Extract keywords and timestamp from the DataFrame
            keywords = ast.literal_eval(df['extracted_keywords'][0])
            print(df.columns)
            timestamp = pd.to_datetime(df['timestamp'][0], unit='ns')

            # Update keyword counts and timestamps
            for keyword, _ in keywords:
                keyword_counts[keyword]['timestamps'].append(timestamp)
                #keyword_counts[keyword]['counts'].append(len(keyword_counts[keyword]['timestamps']))

            # Calculate keyword counts in the specified time periods and store in a DataFrame
            data = []
            for keyword, timestamps in keyword_counts.items():
                row = {'Keyword': keyword}
                for period in [1, 15, 60]:  # Time periods in minutes
                    period_start = timestamp - timedelta(minutes=period)
                    count = sum(t >= period_start for t in timestamps)
                    row[f'{period}_min_count'] = count
                data.append(row)
            count_df = pd.DataFrame(data)
            #print(count_df)
            output_stream = topic_producer.get_or_create_stream("a")
            output_stream.timeseries.publish(count_df)

            # Continue with the existing code...
            #stream_producer = topic_producer.get_or_create_stream(stream_id = stream_consumer.stream_id)
            #stream_producer.timeseries.buffer.publish(df)
        except Exception as e:
            print("An error occurred:")
            traceback.print_exc()
            print(" ")
            print("-----------------------------------------------------------------------")
            print("Disconnecting dataframe handler and stopping app to prevent data loss..")
        
            stream_consumer.events.on_data_received = None
            stream_consumer.timeseries.on_dataframe_received = None
            os.kill(os.getpid(), signal.SIGTERM)
        

# Handle event data from samples that emit event data
def on_event_data_received_handler(stream_consumer: qx.StreamConsumer, data: qx.EventData):
    print(data)
    # handle your event data here


def on_stream_received_handler(stream_consumer: qx.StreamConsumer):
    # subscribe to new DataFrames being received
    # if you aren't familiar with DataFrames there are other callbacks available
    # refer to the docs here: https://docs.quix.io/sdk/subscribe.html
    stream_consumer.events.on_data_received = on_event_data_received_handler # register the event data callback
    stream_consumer.timeseries.on_dataframe_received = on_dataframe_received_handler


# subscribe to new streams being received
topic_consumer.on_stream_received = on_stream_received_handler

print("Listening to streams. Press CTRL-C to exit.")

# Handle termination signals and provide a graceful exit
qx.App.run()
print("Goodbye!")
