##############################################
# 01 - Loading libraries and packages
##############################################
import os
import pandas as pd

import tools.manage_files as mf
import tools.download as dl

##############################################
# 02 - Setting parameters
##############################################

# Setting global parameters
root = "/indicator"
conf_folder = os.path.join(root, "conf")
# Inputs
inputs_folder = os.path.join(root, "inputs")
inputs_f_downloads = os.path.join(inputs_folder, "01_downloads")
inputs_f_raw = os.path.join(inputs_folder, "02_raw")

# Creating folders
mf.mkdir(inputs_folder)
mf.mkdir(inputs_f_downloads)
mf.mkdir(inputs_f_raw)


# Loading configurations
conf_downloads = pd.read_excel(os.path.join(conf_folder, "conf.xlsx"), index_col=0, sheet_name='downloads')  

##############################################
# 03 - Getting data from sources
##############################################

# Getting data from faostat
conf_downloads["status"] = conf_downloads.apply(lambda x: dl.download_url(x.url,os.path.join(inputs_f_downloads,"file")), axis=1)
