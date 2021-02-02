import os
import pathlib
from urllib.parse import urlparse
import requests

TEMP_DIR = os.path.join(os.getcwd(), 'temp')
pathlib.Path(TEMP_DIR).mkdir(exist_ok=True, parents=True)

img_path = "https://blobact.blob.core.windows.net/test-dev/f92cf580-0c9c-11eb-b0d0-5a895a3c96cd%2Fpage-1.jpg"


def save_file(img_path):
    path = urlparse(img_path).path
    file_name = path.split("/")[-1]
    file_path = os.path.join(TEMP_DIR, f'{file_name}')
    file_response = requests.get(img_path)
    with open(file_path, "wb") as f:
        f.write(file_response.content)


save_file(img_path)
