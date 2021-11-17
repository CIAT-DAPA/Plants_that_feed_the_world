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
fao_encoding = conf_general.loc[conf_general["variable"] == "fao_encoding","value"].values[0]
fao_production = conf_general.loc[conf_general["variable"] == "fao_production","value"].values[0]
fao_special_files = conf_general.loc[conf_general["variable"] == "fao_special_files","value"].values[0].split(",")
fao_production_field = conf_general.loc[conf_general["variable"] == "fao_production_field","value"].values[0]
fao_countries_limit = float(conf_general.loc[conf_general["variable"] == "fao_countries_limit","value"].values[0])
fao_countries_suffix =  conf_general.loc[conf_general["variable"] == "fao_countries_suffix","value"].values[0]
fao_element_population =  conf_general.loc[conf_general["variable"] == "fao_element_population","value"].values[0]
fao_years = [2015,2016,2017,2018]
print("FAO encoding: " + fao_encoding)
print("FAO production: " + fao_production)
print("FAO production field: " + fao_production_field)

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
fao_downloaded = conf_downloads.loc[conf_downloads["database"] == "fao","output"]
# Remove Population File
fao_downloaded_files = [file for file in fao_downloaded if "Population" not in file]
fao_downloaded_population = [file for file in fao_downloaded if "Population" in file]
# Process other files
fao.create_workspace(inputs_f_raw)
print("Merging countries")
fao.merge_countries(countries_list, fao_downloaded_files, inputs_f_raw, encoding=fao_encoding)
print("Merging items cleaned")
fao.item_cleaned(crops_xls, os.path.join(inputs_f_raw,"fao","01"), inputs_f_raw, encoding=fao_encoding)
print("Merging with crops")
fao.merge_crops(crops_xls,os.path.join(inputs_f_raw,"fao","02"),inputs_f_raw,encoding=fao_encoding)
print("Summarizing items")
fao.sum_items(crops_xls, os.path.join(inputs_f_raw,"fao","03"), inputs_f_raw, fao_years, encoding=fao_encoding)
print("Calculating groups for commodities")
fao_f_commodities = fao.calculate_commodities(crops_xls, os.path.join(inputs_f_raw,"fao","04"), inputs_f_raw, fao_years, 
                    fao_production, fao_production_field, encoding=fao_encoding)
print("Calculating values for the years")
fao.calculate_values(os.path.join(inputs_f_raw,"fao","04"), inputs_f_raw, fao_years, fao_special_files, 
                    fao_f_commodities, encoding=fao_encoding)
print("Calculating crops contribution by country")
fao.calculate_contribution_crop_country(os.path.join(inputs_f_raw,"fao","06"), inputs_f_raw, fao_years, encoding=fao_encoding)
print("Counting by country")
fao.count_countries(os.path.join(inputs_f_raw,"fao","07"), inputs_f_raw, fao_years, fao_countries_limit, 
                    fao_countries_suffix, encoding=fao_encoding)
print("Calculating population")
fao.calculate_population(countries_xls, fao_downloaded_population, inputs_f_raw, fao_years,fao_element_population, 
                    encoding=fao_encoding)
print("Calculating interdependence")
fao.calculate_interdependence(crops_xls,os.path.join(inputs_f_raw,"fao","06"),inputs_f_raw, fao_years,fao_special_files,
                    os.path.join(inputs_f_raw,"fao","09"))
print("Calculating gini")
fao.calculate_gini(countries_xls, os.path.join(inputs_f_raw,"fao","10"),inputs_f_raw, fao_years, force=True)