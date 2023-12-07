import requests
import zipfile

# Direct download link for the Google Drive file
direct_link = 'https://drive.google.com/uc?id=1eIdeNAOe40JN3Ogz7okoGuW_h7ToYjyj&export=download'

# Path to save the downloaded zip file
zip_file_path = 'r_dataengineering.zip'

# Path to extract the contents of the zip file
extract_path = '.'

# Stream the download (download in chunks)
response = requests.get(direct_link, stream=True)
response.raise_for_status()  # Ensure the download was successful

# Get the total file size in bytes
total_size = int(response.headers.get('content-length', 0))
chunk_size = 1024  # 1 kilobyte
total_downloaded = 0

# Open the file to write as binary - 'wb'
with open(zip_file_path, 'wb') as file:
    for chunk in response.iter_content(chunk_size):
        # Write the chunk to the file
        file.write(chunk)
        # Update the total number of bytes downloaded
        total_downloaded += len(chunk)
        # Print the progress
        print(f'Downloaded {total_downloaded} of {total_size} bytes', end='\r')

# Unzip the file
# with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
#     zip_ref.extractall(extract_path)

# print('Download and extraction complete.')