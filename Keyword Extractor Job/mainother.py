import json
from keybert import KeyBERT
from keyphrase_vectorizers import KeyphraseCountVectorizer
import ray
import modin.pandas as pd
import os
import quixstreams as qx
import gdown
import zipfile
# Initialize Ray
ray.init()

# Initialize KeyBERT model
kw_model = KeyBERT()

# Initialize Kafka producer
client = qx.QuixStreamingClient()

# Download and extract file
url = 'https://drive.google.com/uc?id=1eIdeNAOe40JN3Ogz7okoGuW_h7ToYjyj'
output = './r_dataengineering.zip'
gdown.download(url, output, quiet=False)
zip_file_path = './r_dataengineering.zip'
extract_path = '.'
# Unzip the file
with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
     zip_ref.extractall(extract_path)
print('Download and extraction complete.')

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
    

    # Send processed data to Kafka
    # Set stream ID or leave parameters empty to get stream ID generated.
    stream_producer = topic_producer.get_or_create_stream(row_basic['parent_id'])
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