##############################################
# 01 - Loading libraries and packages
##############################################
import os
import pandas as pd
from raw_data.google import Google
from raw_data.wikipedia import Wikipedia
from raw_data.previous_data import PreviousData
from raw_data.fao_sow import FaoSow
from raw_data.fao_wiews import FaoWiews
from raw_data.mls import MLS
from indicator.indicator import Indicator

os.chdir('/indicator/src')

import tools.manage_files as mf
import tools.download as dl

import raw_data.fao as fao
from raw_data.google import Google

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
inputs_f_indicator = os.path.join(inputs_folder, "03_indicator")
# Logs
logs_folder = os.path.join(data_folder, "logs")

# Creating folders
print("Creating folders")
mf.mkdir(inputs_folder)
mf.mkdir(inputs_f_downloads)
mf.mkdir(inputs_f_raw)
mf.mkdir(inputs_f_indicator)
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
crops_genus_list = crops_xls.parse("crops_genus")
crops_taxa_list = crops_xls.parse("crops_taxa")
crops_common_list = crops_xls.parse("crops_common")
new_names_list = crops_xls.parse("new_names")
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
google_file = conf_general.loc[conf_general["variable"] == "google_file","value"].values[0]
google_sheet = conf_general.loc[conf_general["variable"] == "google_sheet","value"].values[0]
google_field_crop = conf_general.loc[conf_general["variable"] == "google_field_crop","value"].values[0]
google_fields_elements = str(conf_general.loc[conf_general["variable"] == "google_fields_elements","value"].values[0]).split(',')
wikipedia_url = conf_general.loc[conf_general["variable"] == "wikipedia_url","value"].values[0]
wikipedia_timing = conf_general.loc[conf_general["variable"] == "wikipedia_timing","value"].values[0]
wikipedia_years = [int(y) for y in str(conf_general.loc[conf_general["variable"] == "wikipedia_years","value"].values[0]).split(',')]
genebank_fields = str(conf_general.loc[conf_general["variable"] == "genebank_fields","value"].values[0]).split(',')
upov_fields = str(conf_general.loc[conf_general["variable"] == "upov_fields","value"].values[0]).split(',')
gbif_research_fields = str(conf_general.loc[conf_general["variable"] == "gbif_research_fields","value"].values[0]).split(',')
fao_sow_file = conf_general.loc[conf_general["variable"] == "fao_sow_file","value"].values[0]
fao_sow_field_crop = conf_general.loc[conf_general["variable"] == "fao_sow_field_crop","value"].values[0]
fao_sow_field_filter = conf_general.loc[conf_general["variable"] == "fao_sow_field_filter","value"].values[0]
fao_sow_value_filter = conf_general.loc[conf_general["variable"] == "fao_sow_value_filter","value"].values[0]
fao_sow_field_count = conf_general.loc[conf_general["variable"] == "fao_sow_field_count","value"].values[0]
fao_sow_field_year = conf_general.loc[conf_general["variable"] == "fao_sow_field_year","value"].values[0]
fao_sow_years = [int(y) for y in str(conf_general.loc[conf_general["variable"] == "fao_sow_years","value"].values[0]).split(',')]
fao_sow_field_recipient = conf_general.loc[conf_general["variable"] == "fao_sow_field_recipient","value"].values[0]
mls_accessions_file = conf_general.loc[conf_general["variable"] == "mls_accessions_file","value"].values[0]
mls_accessions_year = conf_general.loc[conf_general["variable"] == "mls_accessions_year","value"].values[0]
mls_institutions_file = conf_general.loc[conf_general["variable"] == "mls_institutions_file","value"].values[0]
mls_institutions_year = conf_general.loc[conf_general["variable"] == "mls_institutions_year","value"].values[0]
fao_wiews_file = conf_general.loc[conf_general["variable"] == "fao_wiews_file","value"].values[0]
fao_wiews_field_crop = conf_general.loc[conf_general["variable"] == "fao_wiews_field_crop","value"].values[0]
fao_wiews_field_filter = conf_general.loc[conf_general["variable"] == "fao_wiews_field_filter","value"].values[0]
fao_wiews_value_filter = conf_general.loc[conf_general["variable"] == "fao_wiews_value_filter","value"].values[0]
fao_wiews_fields_elements = str(conf_general.loc[conf_general["variable"] == "fao_wiews_fields_elements","value"].values[0]).split(',')
fao_wiews_field_year = conf_general.loc[conf_general["variable"] == "fao_wiews_field_year","value"].values[0]
fao_wiews_years = [int(y) for y in str(conf_general.loc[conf_general["variable"] == "fao_wiews_years","value"].values[0]).split(',')]
fao_wiews_field_recipient = conf_general.loc[conf_general["variable"] == "fao_wiews_field_recipient","value"].values[0]

##############################################
# 03 - Downloading data from sources
##############################################
print("03 - Downloading data from sources")

# Getting data from faostat
print("Getting data from faostat")
conf_downloads["output"] = conf_downloads.apply(lambda x: dl.download_url(x.url, inputs_f_downloads), axis=1)


##############################################
# 04 - Processing FAO downloaded data
##############################################
print("04 - Processing FAO downloaded data")

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
fao_out_inter = fao.calculate_interdependence(crops_xls,os.path.join(inputs_f_raw,"fao","06"),inputs_f_raw, fao_years,fao_special_files,
                    os.path.join(inputs_f_raw,"fao","09"))
print("Calculating gini")
fao.calculate_gini(countries_xls, os.path.join(inputs_f_raw,"fao","10"),inputs_f_raw, fao_years)
print("Summarizing data for crops through countries")
fao.summarize_data(os.path.join(inputs_f_raw,"fao","06"), inputs_f_raw, fao_years, encoding=fao_encoding)
print("Summarizing data for crops through countries")
fao.extracting_interdependence(fao_out_inter, inputs_f_raw, fao_years, encoding=fao_encoding)

##############################################
# 05 - Processing Google downloaded data
##############################################
print("05 - Processing Google downloaded data")

google = Google()
print("Fixing format of Google data")
google.fix_data(os.path.join(inputs_f_downloads,google_file),inputs_f_raw,google_sheet,google_field_crop,google_fields_elements)

##############################################
# 06 - Processing Wikipedia data
##############################################
print("06 - Processing Wikipedia data")

wikipedia = Wikipedia(url=wikipedia_url,timing=wikipedia_timing,years=wikipedia_years)
print("Getting wikipedia views data")
wikipedia.get_pageviews(inputs_f_raw,crops_genus_list,crops_taxa_list, crops_common_list)

##############################################
# 07 - Processing Genebank data
##############################################
print("07 - Processing Genebank data")

genebank = PreviousData(final_file="genebank_collection.csv")
print("Checking and fixing genebanks collection data")
genebank.check_and_fix_data(os.path.join(inputs_f_downloads,"genebank_collections.csv"), inputs_f_raw, 
        "genebank", crops_list,  new_names_list, genebank_fields)

##############################################
# 08 - Processing Upov data
##############################################
print("08 - Processing Upov data")

upov = PreviousData(final_file="upov_varietal_release.csv")
print("Checking and fixing UPOV data")
upov.check_and_fix_data(os.path.join(inputs_f_downloads,"upov_varietal_release.csv"), inputs_f_raw, 
        "upov", crops_list,  new_names_list, upov_fields)

##############################################
# 09 - Processing GBIF Research data
##############################################
print("09 - Processing GBIF Research data")

upov = PreviousData(final_file="gbif_research_supply.csv")
print("Checking and fixing GBIF data")
upov.check_and_fix_data(os.path.join(inputs_f_downloads,"gbif_research_supply.csv"), inputs_f_raw, 
        "gbif", crops_list,  new_names_list, gbif_research_fields)

##############################################
# 10 - Processing FAO SOW III Study Treaty
##############################################
print("10 - Processing FAO SOW III Study Treaty")

fao_sow = FaoSow(fao_sow_field_crop, fao_sow_field_filter, fao_sow_value_filter, fao_sow_field_count, fao_sow_field_year,
                        fao_sow_years, fao_sow_field_recipient, inputs_f_raw)
print("Filtering, counting and selection data")
fao_sow_input = fao_sow.filter_sum_distribution(os.path.join(inputs_f_downloads,fao_sow_file))
print("Counting recipients countries")
fao_sow.count_country_recipients(fao_sow_input)
print("Calculating interdependence")
fao_sow_interdependence = fao_sow.calculate_interdependence(fao_sow_input,crops_xls,os.path.join(inputs_f_raw,"fao","09"))
print("Calculating gini")
fao_sow.calculate_gini(fao_sow_interdependence, countries_xls)
print("Sum transfers")
fao_sow.sum_transfers(fao_sow_input)

##############################################
# 11 - Processing FAO SOW III Study WIEWS
##############################################
print("11 - Processing FAO SOW III Study WIEWS")

fao_wiews = FaoWiews(fao_wiews_field_crop, fao_wiews_field_filter, fao_wiews_value_filter, fao_wiews_fields_elements, 
                        fao_wiews_years, fao_wiews_field_recipient, inputs_f_raw)
print("Filtering, counting and selection data")
fao_wiews_input = fao_wiews.filter_sum_distribution(os.path.join(inputs_f_downloads,fao_wiews_file),fao_wiews_field_year)

##############################################
# 12 - Processing MLS
##############################################
print("12 - Processing MLS")
mls_accessions = MLS('mls_accessions.csv',inputs_f_raw,"mls_accesions")
mls_accessions.extract_data(os.path.join(inputs_f_downloads,mls_accessions_file),mls_accessions_year)

mls_institutions = MLS('mls_institutions.csv',inputs_f_raw,"mls_institutions")
mls_institutions.extract_data(os.path.join(inputs_f_downloads,mls_institutions_file),mls_institutions_year)

##############################################
# 13 - Indicator
##############################################


#indicator = Indicator()