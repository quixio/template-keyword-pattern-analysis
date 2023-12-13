import json
from bertopic import BERTopic
from bertopic.representation import KeyBERTInspired
import pandas as pd
import gdown
import zipfile

# Download and extract file
url = 'https://drive.google.com/uc?id=1jVUMJPfirdxHviPiWkeOuNmIzc97QLPV'
output = './r_dataengineering_comments.zip'
gdown.download(url, output, quiet=False)
zip_file_path = output
extract_path = '.'

# Unzip the file
with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
     zip_ref.extractall(extract_path)
print('Download and extraction complete.')

# We select a subsample of the archive or simply all
data = []
with open('./r_dataengineering_comments.jsonl', 'r', encoding='utf-8') as file:
    for line in file:
        # Convert each line to a dictionary
        json_data = json.loads(line)
        data.append(json_data)

# Create DataFrame from list of dictionaries
df = pd.DataFrame(data)
df_basic = df[['created_utc','parent_id','author','body']].copy()
df_basic['human_readable_time'] = pd.to_datetime(df_basic['created_utc'], unit='s')

docs = df_basic["body"][0:5000]

# We define a number of topics that we know are in the documents
zeroshot_topic_list = ["Kafka", "Flink", "Spark"]

# We fit our model using the zero-shot topics
# and we define a minimum similarity. For each document,
# if the similarity does not exceed that value, it will be used
# for clustering instead.
topic_model = BERTopic(
    embedding_model="thenlper/gte-small", 
    min_topic_size=15,
    zeroshot_topic_list=zeroshot_topic_list,
    zeroshot_min_similarity=.85,
    representation_model=KeyBERTInspired()
)
topics, probs = topic_model.fit_transform(docs)

topic_model.get_topic_info()

embedding_model = "thenlper/gte-small"
topic_model.save("./state/deng_model_dir", serialization="safetensors", save_ctfidf=False, save_embedding_model=embedding_model)