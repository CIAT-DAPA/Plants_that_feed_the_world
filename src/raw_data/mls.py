import os
import re
import glob

from pandas.core import groupby
import numpy as np
import pandas as pd
import tools.manage_files as mf

# Class with process data from FAO SOW III Study
class MLS(object):

    encoding = ''
    folder = ''
    file = ''
    root_path = ''

    ## Method construct
    def __init__(self, file, root_path, folder,  encoding="ISO-8859-1"):
        self.file = file
        self.folder = folder
        self.encoding = encoding
        self.root_path = root_path

    # Method that arrange the raw data
    # (dataframe) df: Dataframe with raw data
    # (string) name: Name of element. Varname
    # (string) year: Year of when is data. it will be the column name
    def fix_init_data(self, df, name, year):
        df_tmp = df[["crop","included"]]
        df_tmp.columns = ["crop","Y" + str(year)]
        df_tmp["Element"] = name
        df_final = df_tmp[["crop","Element","Y" + str(year)]]
        return df_final


    # Method that extracts data from file and adjust in format for the analysis
    # (string) location:  String with the path of where the system should take the files.
    #                   It will filter all csv files from the path.
    #                   It just will process the OK files
    # (string) step: prefix of the output files. By default it is 01
    # (bool) force: Set if the process have to for the execution of all files even if the were processed before. 
    #               By default it is False
    def extract_data(self, location, year, sheets=["genus","species"], step="01",force = False):
        final_path = os.path.join(self.root_path,self.folder,step)
        mf.create_review_folders(final_path, er=False,sm=False)

        final_file = os.path.join(final_path,"OK",self.file)
        # It checks if files should be force to process again or if the path exist
        if force or not os.path.exists(final_file):
            print("\tWorking with: " + location)
            data_xls = pd.ExcelFile(location)
            df = pd.DataFrame()
            # Loop for extract data from genus and species sheets
            for s in sheets:
                var_name = self.file.replace(".csv","") + "_" + s
                df_sheet = self.fix_init_data(data_xls.parse(s),var_name,year)
                if df.shape[0] == 0:
                    df = df_sheet
                else:
                    df = df.append(df_sheet)

            df.to_csv(final_file, index = False, encoding = self.encoding)

        else:
            print("\tNot processed: " + final_file)

        return final_file
