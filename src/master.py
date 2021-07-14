##############################################
# 01 - Loading libraries and packages
##############################################
import os
import pandas as pd

os.chdir('/indicator/src')

import tools.manage_files as mf
import tools.download as dl

##############################################
# 02 - Setting configuration
##############################################

# Setting global parameters
root = "/indicator"
data_folder = os.path.join(root, "data")
conf_folder = os.path.join(data_folder, "conf")
# Inputs
inputs_folder = os.path.join(data_folder, "inputs")
inputs_f_downloads = os.path.join(inputs_folder, "01_downloads")
inputs_f_raw = os.path.join(inputs_folder, "02_raw")

# Creating folders
mf.mkdir(inputs_folder)
mf.mkdir(inputs_f_downloads)
mf.mkdir(inputs_f_raw)

# Loading configurations
conf_xls = pd.ExcelFile(os.path.join(conf_folder,"conf.xlsx"))
conf_downloads = conf_xls.parse("downloads")

# Loading parameters
countries = pd.read_csv(os.path.join(conf_folder, "countries.csv"), encoding = "UTF-8")
crops = pd.read_csv(os.path.join(conf_folder, "crops.csv"), encoding = "UTF-8")

##############################################
# 03 - Downloading data from sources
##############################################

# Getting data from faostat
conf_downloads["status"] = conf_downloads.apply(lambda x: dl.download_url(x.url,inputs_f_downloads), axis=1)
