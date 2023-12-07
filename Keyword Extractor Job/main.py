import requests
import zipfile
import gdown

# url = 'https://drive.google.com/uc?id=1eIdeNAOe40JN3Ogz7okoGuW_h7ToYjyj'
#url = 'https://drive.google.com/file/d/1eIdeNAOe40JN3Ogz7okoGuW_h7ToYjyj/view?usp=drive_link'
# output = 'r_dataengineering.zip'
# gdown.download(url, output, quiet=False)

zip_file_path = 'r_dataengineering.zip'
extract_path = '.'

# Unzip the file
with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
     zip_ref.extractall(extract_path)

print('Download and extraction complete.')