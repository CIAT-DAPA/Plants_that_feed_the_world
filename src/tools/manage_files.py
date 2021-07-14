
import os

# Function which creates a folder. It checks if the folders exist before
# (string) path: Path where the folder should be create
def mkdir(path):
    if not os.path.exists(path): 
        os.mkdir(path)