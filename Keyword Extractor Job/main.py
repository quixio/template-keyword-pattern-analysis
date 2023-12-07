import json
from keybert import KeyBERT
from keyphrase_vectorizers import KeyphraseCountVectorizer
import ray
import modin.pandas as pd
import os
import quixstreams as qx

# Initialize Ray
ray.init()

# Initialize KeyBERT model
kw_model = KeyBERT()

# Initialize Kafka producer
client = qx.QuixStreamingClient()

# Open the output topic where to write data out
topic_producer = client.get_topic_producer(topic_id_or_name = os.environ["output"])


def extract_keywords_with_error_handling(doc):
    try:
        return kw_model.extract_keywords(doc, vectorizer=KeyphraseCountVectorizer(pos_pattern='<N.*>+'))
    except Exception as e:
        print(f"Error because: {e}")
        return None

# Function to process and send each row to Kafka
def process_and_send(row):
    # Process the row
    row_basic = row[['created_utc', 'parent_id', 'author', 'body']].copy()
    row_basic  = row_basic .rename(columns={"created_utc": "timestamp"})
    row_basic['human_timestamp'] = pd.to_datetime(row_basic['timestamp'], unit='s')
    row_basic['word_count'] = row_basic['body'].str.split().apply(len)
    row_basic['extracted_keywords'] = extract_keywords_with_error_handling(row_basic['body'])
    
    # Convert row to dictionary and add process ID
    processed_data = row_basic.to_dict('records')[0]
    processed_data['process_id'] = os.getpid()
    
    # Send processed data to Kafka
    # Set stream ID or leave parameters empty to get stream ID generated.
    stream_producer = topic_producer.get_or_create_stream(row_basic[])
    stream_producer.properties.name = "Keyword Extraction"

     # publish the data to the Quix stream created earlier
    stream_producer.timeseries.publish(row_basic)

# Read the JSONL file and process each line
with open('r_dataengineering_comments.jsonl', 'r', encoding='utf-8') as file:
    for line in file:
        json_data = json.loads(line)
        df_row = pd.DataFrame([json_data])
        process_and_send(df_row)

# Ensure all messages are sent before closing the producer
print("Closing stream")
stream.close()