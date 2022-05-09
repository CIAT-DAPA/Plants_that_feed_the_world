import os
import re
import glob

from pandas.core import groupby
import numpy as np
import pandas as pd
import tools.manage_files as mf
import tools.interdependence as id
import tools.gini as gi


# Class with process data from FAO SOW III Study
class FaoWiews(object):

    encoding = ''
    folder = ''
    file = ''
    field_crop = ''
    field_filter = ''
    value_filter = ''
    fields_elements = []
    years = ''
    field_recipient = ''
    nan = ''
    root_path = ''


    ## Method construct
    def __init__(self, field_crop, field_filter, value_filter, fields_elements, years, field_recipient, root_path, file='fao_wiews.csv', folder='fao_wiews', nan='None', encoding="ISO-8859-1"):
        self.field_crop = field_crop
        self.field_filter = field_filter
        self.value_filter = value_filter
        self.fields_elements = fields_elements
        self.years = years
        self.field_recipient = field_recipient
        self.root_path = root_path
        self.nan = nan
        self.file = file
        self.folder = folder
        self.encoding = encoding

    # Method that filter data for specific records,
    # further it select the fields needed, finally it aggregates the transfers
    # of accessions by crop, country and year`
    # (string) location:  String with the path of where the system should take the files.
    #                   It will filter all csv files from the path.
    #                   It just will process the OK files`
    # (string) field_year: Name field which contains the year value
    # (string) step: prefix of the output files. By default it is 01
    # (bool) force: Set if the process have to for the execution of all files even if the were processed before. 
    #               By default it is False
    def filter_sum_distribution(self, location, field_year, step="01",force = False):
        final_path = os.path.join(self.root_path,self.folder,step)
        mf.create_review_folders(final_path, er=False,sm=False)

        final_file = os.path.join(final_path,"OK",self.file)
        # It checks if files should be force to process again or if the path exist
        if force or not os.path.exists(final_file):
            print("\tWorking with: " + location)
            df = pd.read_csv(location, encoding = self.encoding)

            print("\tFiltering data")
            df = df.loc[df[self.field_filter] == self.value_filter,:]
            df = df.loc[df[self.field_crop] != self.nan,:]
            df = df.loc[df[field_year].isin(self.years),:]
            print("\tSelecting data")
            df = df[[self.field_crop,self.field_recipient,field_year] + self.fields_elements]
            df.columns = ["crop","country","year"] + self.fields_elements
            df["year"] = "Y" + df["year"].astype(str)
            df = df.groupby(["crop","year"], as_index=False)[self.fields_elements].sum()
            df_final = pd.DataFrame()
            # Loop which integrates many columns in just one with many rows (pivot)
            for e in self.fields_elements:
                df_tmp = df[["crop","year",e]]
                df_tmp.columns = ["crop","year","value"]
                df_tmp["Element"] = e
                df_final = df_tmp if df_final.shape[0] == 0 else df_final.append(df_tmp,ignore_index=True)
            df = df_final
            # Pivot by years
            df = df.pivot_table(index=["crop","Element"], columns=["year"], values="value", aggfunc=np.sum)
            df.reset_index(level=["crop","Element"], inplace=True)
            df.to_csv(final_file, index = False, encoding = self.encoding)

        else:
            print("\tNot processed: " + final_file)

        return final_file

    # Method that filter data for specific records,
    # further it select the fields needed, finally it aggregates the transfers
    # of accessions by crop, country and year
    # (string) location:  String with the path of where the system should take the files.
    #                   It will filter all csv files from the path.
    #                   It just will process the OK files
    # (string) step: prefix of the output files. By default it is 02
    # (bool) force: Set if the process have to for the execution of all files even if the were processed before. 
    #               By default it is False
    def count_country_recipients(self, location, step="02",force = False):
        final_path = os.path.join(self.root_path,self.folder,step)
        mf.create_review_folders(final_path, er=False,sm=False)

        final_file = os.path.join(final_path,"OK",self.file)
        # It checks if files should be force to process again or if the path exist
        if force or not os.path.exists(final_file):
            print("\tWorking with: " + location)
            df = pd.read_csv(location, encoding = self.encoding)
            df_final = pd.DataFrame()
            df_final["crop"] = df["crop"].drop_duplicates()

            for y in self.years:
                print("\t\tCounting",y)
                y_name = "Y" + str(y)
                df_tmp = df[["crop","country",y_name]]
                df_tmp = df_tmp[df_tmp[y_name].notna()]
                df_tmp = df_tmp.groupby(["crop"], as_index=False).size()
                df_tmp.columns = ["crop",y_name] 
                df_final = pd.merge(df_final,df_tmp,on=["crop"],how="left")

            df_final["Element"] = "countries_recipients"
            df_final.to_csv(final_file, index = False, encoding = self.encoding)

        else:
            print("\tNot processed: " + final_file)

    #  Method that calculates the interdependence. It returns the path where OK files are located
    # (string) location:  String with the path of where the system should take the files.
    #                   It will filter all csv files from the path.
    #                   It just will process the OK files
    # (XLSParse) conf_crops: XLS Parse object which has the configurations for crops
    # (string) population: Path where the population files are. 
    #                   It should have the three files: population by country, region and segregated
    # (string) step: prefix of the output files. By default it is 03
    # (bool) force: Set if the process have to for the execution of all files even if the were processed before. 
    #               By default it is False
    def calculate_interdependence(self, location, conf_crops, population, step="03",force = False):
        final_path = os.path.join(self.root_path,self.folder,step)
        mf.create_review_folders(final_path)
        # Calculate the total regions to fill the dataset with zeros in the regions which the crop is not
        region_crops = conf_crops.parse("regions")
        y_years = ["Y" + str(x) for x in self.years]
        # Loading the population files
        df_population_region = pd.read_csv(os.path.join(population,"SM","population_region.csv"), encoding = self.encoding)
        df_population_segregated = pd.read_csv(os.path.join(population,"SM","population_segregated.csv"), encoding = self.encoding)
        final_file = os.path.join(final_path,"SM",self.file)
        # It checks if files should be force to process again or if the path exist
        if force or not os.path.exists(final_file):
            method = "sum"

            print("\tWorking with:",location,"Method:",method)
            df = pd.read_csv(location, encoding = self.encoding)
            df["Element"] = "samples"
            df_inter = id.interdependence(df,region_crops,method,y_years,final_path,self.file.replace(".csv",""),df_population_region,df_population_segregated)
            print("\tSaving output")
            df_inter.to_csv(final_file, index = False, encoding = self.encoding)

        return os.path.join(final_path,"OK")
    
    # Method that calculates the gini indicator to raw data    
    # (string) location: String with the path of where the system should take the files.
    #                   It will filter all csv files from the path.
    #                   It just will process the OK files
    # (XLSParse) conf_countries: XLS Parse object which has the configurations for countries
    # (string) step: prefix of the output files. By default it is 10
    # (bool) force: Set if the process have to for the execution of all files even if the were processed before. 
    #               By default it is False
    def calculate_gini(self, location, conf_countries, step="04",force=False):
        final_path = os.path.join(self.root_path,self.folder,step)
        mf.create_review_folders(final_path)

        y_years = ["Y" + str(x) for x in self.years]
        # Calculate the total regions to fill the dataset with zeros in the regions which the crop is not
        regions = conf_countries.parse("regions")
        regions_total = len(regions["region"].unique())
        # Get files to process
        folders = glob.glob(os.path.join(location, "*/"))
        print(os.path.join(location,'OK', "*/"))
        for folder in folders:
            f_name = folder.rsplit(os.path.sep)[-2]
            final_file = os.path.join(final_path,"OK",f_name + ".csv")
            # It checks if files should be force to process again or if the path exist        
            if force or not os.path.exists(final_file):

                print("\tWorking with:",folder)
                df = pd.read_csv(os.path.join(folder,"01-summary_element_crop_region.csv"), encoding = self.encoding)   
                df_gini = gi.gini_crop(df,y_years,regions_total)

                print("\tSaving output")
                df_gini["Element"] = "gini_sample_recipients"
                df_gini.to_csv(final_file, index = False, encoding = self.encoding)
    
    # Method that filter data for specific records,
    # further it select the fields needed, finally it aggregates the transfers
    # of accessions by crop, country and year
    # (string) location:  String with the path of where the system should take the files.
    #                   It will filter all csv files from the path.
    #                   It just will process the OK files
    # (string) step: prefix of the output files. By default it is 02
    # (bool) force: Set if the process have to for the execution of all files even if the were processed before. 
    #               By default it is False
    def sum_transfers(self, location, step="05",force = False):
        final_path = os.path.join(self.root_path,self.folder,step)
        mf.create_review_folders(final_path, er=False,sm=False)

        y_years = ["Y" + str(x) for x in self.years]
        final_file = os.path.join(final_path,"OK",self.file)
        # It checks if files should be force to process again or if the path exist
        if force or not os.path.exists(final_file):
            print("\tWorking with: " + location)
            df = pd.read_csv(location, encoding = self.encoding)
            df = df.groupby(["crop"], as_index=False)[y_years].sum()

            df["Element"] = "samples"
            df.to_csv(final_file, index = False, encoding = self.encoding)

        else:
            print("\tNot processed: " + final_file)
