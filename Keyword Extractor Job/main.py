import requests
import zipfile

def download_file_from_google_drive(id, destination):
    URL = "https://drive.google.com/uc?export=download"

    session = requests.Session()
    response = session.get(URL, params={'id': id}, stream=True)
    token = get_confirm_token(response)

    if token:
        params = {'id': id, 'confirm': token}
        response = session.get(URL, params=params, stream=True)

    save_response_content(response, destination)

def get_confirm_token(response):
    for key, value in response.cookies.items():
        if key.startswith('download_warning'):
            return value
    return None

def save_response_content(response, destination):
    CHUNK_SIZE = 32768

    with open(destination, "wb") as f:
        for chunk in response.iter_content(CHUNK_SIZE):
            if chunk:  # filter out keep-alive new chunks
                f.write(chunk)

# Google Drive ID of the file
file_id = '1eIdeNAOe40JN3Ogz7okoGuW_h7ToYjyj'
# Destination where you want to save the zip file
destination = 'r_dataengineering.zip'

download_file_from_google_drive(file_id, destination)

# Unzip the file
#with zipfile.ZipFile(destination, 'r') as zip_ref:
#    zip_ref.extractall('.')
