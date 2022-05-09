from curses import raw
import os
import re
import glob

from pandas.core import groupby
import numpy as np
import pandas as pd
import tools.manage_files as mf

class Indicator(object):

    output_folder=''
    encoding=''

    # Method construct
    def __init__(self,output_folder,encoding="ISO-8859-1"):
        self.output_folder = output_folder
        self.encoding = encoding

    # Method that join all datasets in just one file, further it standarize the name of fields
    # and create and average through years. For the averga it just takes into account fields with data
    # fields without data won't take into account
    # (string) folder_raw: Folder where raw data is located
    # (XLSParse) conf_indicator: Configuration for all metrics
    def extract_raw_data(self, folder_raw, conf_indicator, years,step="01",force=False):
        final_path = os.path.join(self.output_folder,step)
        mf.create_review_folders(final_path, er=False,sm=False)
        conf = conf_indicator.parse("indicator")
        # Create global dataframe
        df = pd.DataFrame()
        # Loop for each configuration, it means foreach metrics
        for index, row in conf.iterrows():
            print("\tWorking with: ")
            raw_file = os.path.join(folder_raw,row["source"],row["step"],row["folder"],row["file"])
            print("\t\tRaw data",raw_file,row["element"])
            print("\t\tIndicator",row["domain"],row["component"],row["group"],row["metric"],row["prefix_year"])
            df_element = pd.read_csv(raw_file, encoding = self.encoding)
            col_element = df_element.columns
            # Check if years have prefix
            print(col_element.to_series().str.contains('x'))
