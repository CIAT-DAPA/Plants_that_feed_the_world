import os
import requests
import zipfile
import tools.processing_bar as pb
import tools.manage_files as mf

# Method that download file from url and unzip the file
# (string) url: Resource's url
# (string) path: Path where the file should be save
# (bool) force: True if you want to replace the current files. By default it is False
def download_url(url, path, force = False):
    # Download file
    file_name = url.rsplit('/', 1)[-1]
    full_path = os.path.join(path,file_name)
    final_path =  os.path.join(path,file_name.rsplit('.', 1)[0])
    # Check if the file should be remove
    if force or not os.path.exists(full_path):
        # remove current files if exist
        if os.path.exists(full_path):
            os.remove(full_path)
            os.remove(final_path)
        with open(full_path, 'wb') as f:
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
        print("\tExtracting: " + full_path)        
        mf.mkdir(final_path)
        with zipfile.ZipFile(full_path,"r") as zip_ref:
            zip_ref.extractall(final_path)

        return full_path
        
    else:
        print("\tFile exists, not downloaded: " + url)
        return final_path
