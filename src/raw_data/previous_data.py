import os
import re
import glob

from pandas.core import groupby
from requests.models import Response
import numpy as np
import pandas as pd
import tools.manage_files as mf
import requests



class PreviousData(object):

    encoding = ''
    final_file = ''

    ## Method construct
    def __init__(self, final_file = "", encoding="ISO-8859-1"):
        self.final_file = final_file
        self.encoding = encoding
    
    # Method that validates datasource
    # (string) step: prefix of the output files. By default it is 01
    # (bool) force: Set if the process have to for the execution of all files even if the were processed before. 
    #               By default it is False
    def check_and_fix_data(self, location, path, folder, crops,  new_names, fields, step="01",force = False):
        final_path = os.path.join(path,folder,step)
        mf.create_review_folders(final_path)

        final_file = os.path.join(final_path,"SM",self.final_file)
        # It checks if files should be force to process again or if the path exist
        if force or not os.path.exists(final_file):
            print("\tWorking with: " + location)
            df = pd.read_csv(location, encoding = self.encoding)
            
            print("\tUpdating new crops names")
            df = pd.merge(df,new_names,how='left',left_on="crop", right_on="old")            
            df.loc[~df["new"].isna(),"crop"] = df.loc[~df["new"].isna(),:]["new"]
            df.drop(["new","old"],axis=1,inplace=True)

            print("\tSearching crops not found")
            crops_not_found = df[(~df["crop"].isin(crops["crop"]))]
            if crops_not_found.shape[0] > 0:
                err_file = os.path.join(final_path,"ER",self.final_file)
                print("\tSaving errors: " + err_file + " (" + str(crops_not_found.shape[0]) + ")")
                crops_not_found.to_csv(err_file, index = False, encoding = self.encoding)
            
            print("\tFilling standar crops")
            # It check which crops from final list are not include in the raw data and then it adds all of them
            new_crops = crops[(~crops["crop"].isin(df["crop"]))]
            if new_crops.shape[0] > 0:
                new_crops_ls = new_crops["crop"].drop_duplicates()
                print("\t\tFound",len(new_crops_ls),"new crops")
                # Loop for each crop
                for row in new_crops_ls:
                    # Loop for each year
                    for y in df["year"].drop_duplicates():
                        df.loc[df.shape[0]] = [row, "", y] + [0 for x in range(len(df.columns) - 3)]
            
            print("\tSaving file in OK for checking values")
            df.to_csv(os.path.join(final_path,"OK",self.final_file), index = False, encoding = self.encoding)
            
            print("\tFixing final format")
            df_final = pd.melt(df, id_vars=['crop','year'], value_vars=fields)
            df_final.columns = ["crop","year","Element","value"]
            df_final["year"] = "Y" + df_final["year"].astype(str)
            df_final = df_final.pivot(index=["crop","Element"], columns="year", values="value")
            df_final = df_final.reset_index()
            df_final = df_final.fillna(0)

            print("\tSaving SM: " + final_file)
            df_final.to_csv(final_file, index = False, encoding = self.encoding)

        else:
            print("\tNot processed: " + final_file)
