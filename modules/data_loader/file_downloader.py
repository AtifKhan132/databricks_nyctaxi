import urllib.request
import os
import shutil

def download_file(url: str, dir_path: str, local_path: str):
    """
    Dowload a file from the given URL to the local _path
    """
    response = urllib.request.urlopen(url)

    os.makedirs(dir_path, exist_ok=True)
        
    with open(local_path, 'wb') as out_file:
        shutil.copyfileobj(response, out_file)