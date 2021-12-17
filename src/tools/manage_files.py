
import os

# Function which creates a folder. It checks if the folders exist before
# (string) path: Path where the folder should be create
def mkdir(path):
    if not os.path.exists(path): 
        os.mkdir(path)

# Method which creates the folders OK, ER, SM for each step
# (string) path: Path where the folders should be create
# (bool) ok: Create folder OK
# (bool) er: Create folder ER
# (bool) sm: Create folder SM
def create_review_folders(path, ok=True, er=True, sm=True):
    mkdir(path)
    if ok:
        mkdir(os.path.join(path,"OK"))
    if er:
        mkdir(os.path.join(path,"ER"))
    if sm:
        mkdir(os.path.join(path,"SM"))