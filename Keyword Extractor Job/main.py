import requests
import zipfile

# Direct download link for the Google Drive file
direct_link = 'https://drive.google.com/uc?id=1eIdeNAOe40JN3Ogz7okoGuW_h7ToYjyj&export=download'

# Path to save the downloaded zip file
zip_file_path = 'r_dataengineering.zip'

# Path to extract the contents of the zip file
extract_path = '.'

# Download the file
response = requests.get(direct_link)
response.raise_for_status()  # Ensure the download was successful

# Save the file to storage
with open(zip_file_path, 'wb') as file:
    file.write(response.content)
    print("wrote the file")

# Unzip the file
#with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
#    zip_ref.extractall(extract_path)
