import os
import requests
import zipfile
import tools.processing_bar as pb

# Method that download file from url and unzip the file
# (string) url: Resource's url
# (string) path: Path where the file should be save
def download_url(url, path):
    # Download file
    with open(path, 'wb') as f:
        print("\tDownloading: " + url)
        response = requests.get(url, stream=True)
        total_length = response.headers.get('content-length')

        if total_length is None: # no content length header
            f.write(response.content)
        else:
            dl = 0
            total_length = int(total_length)
            for data in response.iter_content(chunk_size=4096):
                dl += len(data)
                f.write(data)
                pb.progress(total_length, dl)
    # Unzip process
    with zipfile.ZipFile(path,"r") as zip_ref:
        print("\tExtracting: " + path)
        zip_ref.extractall(path)    
