##############################################
# 01 - Loading libraries and packages
##############################################
import os
import pandas as pd

os.chdir('/indicator/src')

import tools.manage_files as mf
import tools.download as dl

import raw_data.fao as fao

##############################################
# 02 - Setting configuration
##############################################
print("02 - Setting configuration")
# Setting global parameters
root = "/indicator"
data_folder = os.path.join(root, "data")
conf_folder = os.path.join(data_folder, "conf")
# Inputs
inputs_folder = os.path.join(data_folder, "inputs")
inputs_f_downloads = os.path.join(inputs_folder, "01_downloads")
inputs_f_raw = os.path.join(inputs_folder, "02_raw")
# Logs
logs_folder = os.path.join(data_folder, "logs")

# Creating folders
print("Creating folders")
mf.mkdir(inputs_folder)
mf.mkdir(inputs_f_downloads)
mf.mkdir(inputs_f_raw)
mf.mkdir(logs_folder)

# Loading configurations
print("Loading configurations")
conf_xls = pd.ExcelFile(os.path.join(conf_folder,"conf.xlsx"))
conf_downloads = conf_xls.parse("downloads")
conf_metrics = conf_xls.parse("metrics")
conf_general = conf_xls.parse("general")

print("Loading countries")
countries_xls = pd.ExcelFile(os.path.join(conf_folder,"countries.xlsx"))
countries_list = countries_xls.parse("countries")

print("Loading crops")
crops_xls = pd.ExcelFile(os.path.join(conf_folder,"crops.xlsx"))
crops_list = crops_xls.parse("crops")

# Extracting global parameters
print("Extracting global parameters")
fao_encoding = conf_general.loc[conf_general["variable"] == "fao_encoding","value"][0]
fao_production = conf_general.loc[conf_general["variable"] == "fao_production","value"][0]
print("FAO encoding: " + fao_encoding)

##############################################
# 03 - Downloading data from sources
##############################################
print("03 - Downloading data from sources")

# Getting data from faostat
print("Getting data from faostat")
conf_downloads["output"] = conf_downloads.apply(lambda x: dl.download_url(x.url, inputs_f_downloads), axis=1)


##############################################
# 04 - Processing downloaded data
##############################################
print("04 - Processing downloaded data")

# Processing fao data
fao_downloaded_files = conf_downloads.loc[conf_downloads["database"] == "fao","output"]
fao.create_workspace(inputs_f_raw)
print("Merging countries")
fao.merge_countries(countries_list, fao_downloaded_files, inputs_f_raw, encoding=fao_encoding)
print("Merging crops")
fao.item_cleaned(crops_xls, os.path.join(inputs_f_raw,"fao","01"), inputs_f_raw, encoding=fao_encoding)
print("Summarizing items")
fao.sum_items(crops_xls, os.path.join(inputs_f_raw,"fao","02"), inputs_f_raw, [2015,2016,2017,2018], encoding=fao_encoding)
#print("Calculating groups for commodities")
#import raw_data.fao as fao
#fao.calculate_commodities(crops_xls, inputs_f_raw, encoding=fao_encoding)

