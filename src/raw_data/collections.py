import os
import re
import glob

from pandas.core import groupby
from requests.models import Response
import numpy as np
import pandas as pd
import tools.manage_files as mf
import requests



class Collection(object):
    
    output = ''
    encoding = ''

    ## Method construct
    def __init__(self, output, encoding="ISO-8859-1"):
        self.output = output
        self.encoding = encoding


    # Method that extracts data from source
    # (dataframe) left:
    # (dataframe) right:
    # (string[]) left_fields:
    # (string[]) right_fields:
    # (string) year:
    # (string) column_year:
    # (string[]) fields:
    # (string) type:
    def merging_data(self, left, right, left_fields, right_fields, year,column_year, fields, type):
        df_final = pd.DataFrame()
        df = pd.merge(left,right,how='inner',left_on=left_fields,right_on=right_fields)
        for field in fields:
            print("\t\tExtracting data for: ",field,year,column_year)
            df_tmp = df
            if year:
                df_tmp["year"] = "Y" + str(year)
                df_tmp["Element"] = field +"_"+ type
                df_tmp = df_tmp[["crop","Element","year", field]]
                df_tmp.columns = ["crop","Element","year", "value"]
            else:
                df_tmp["Element"] = field +"_"+ type
                df_tmp = df_tmp[["crop","Element",column_year, field]]
                df_tmp[column_year] = "Y" + df_tmp[column_year].astype(str)
                df_tmp.columns = ["crop","Element","year","value"]
                
            df_final = df_tmp if df_final.shape[0] == 0 else df_final.append(df_tmp,ignore_index=True)
        return df_final

    # Method that 
    # It generates two types of files: in OK you can see by each variable per year and
    # the SM you can extract the final file
    # (string) file: Source file. It should be excel file    
    # (string) path: Location where the files should be saved    
    # (dataframe) crops_genus: Dataframe with the list of crops and their genus
    # (dataframe) taxa: Dataframe with the list of crops and their taxa
    # (dataframe) new_names: Some updates of names to take into acocunt
    # (string) step: prefix of the output files. By default it is 01
    # (bool) force: Set if the process have to for the execution of all files even if the were processed before. 
    #               By default it is False
    def extract_data(self, file_src, sheet_src, path_out, crops_genus, taxa, new_names, genus, taxon, fields, add_value=False, year = None, column_year=None, step="01",force = False):
        final_path = os.path.join(path_out,self.output,step)
        mf.create_review_folders(final_path,er=False,sm=False)
        final_file = os.path.join(final_path,"OK",self.output + ".csv")
        # It checks if files should be force to process again or if the path exist
        if force or not os.path.exists(final_file):
            print("\tWorking with: " + file_src)

            df = pd.DataFrame()

            data_src = pd.ExcelFile(file_src)
            df_src = data_src.parse(sheet_src)
            # Create the list of fields that should be count
            final_fields = fields
            # It validates if we should create a new field for count records
            # it is just needed when source doesn't have column for values and system should sum records
            if add_value:
                df_src["value"] = 1
                final_fields = ["value"]
            # Extracting data by genus
            print("\tExtracting by genus",final_fields)
            df = self.merging_data(crops_genus,df_src,['genera'],[genus],year,column_year, final_fields, "genus")
            # Extracting data by taxa
            print("\tExtracting by taxa",final_fields)
            df_tmp = self.merging_data(taxa,df_src,['taxa'],[taxon],year,column_year, final_fields, "taxon")

            df = df.append(df_tmp, ignore_index=True)

            # Fixing crops names
            df = pd.merge(df,new_names,how='left',left_on="crop", right_on="old")
            df.loc[~df["new"].isna(),"crop"] = df.loc[~df["new"].isna(),:]["new"]
            df.drop(["new","old"],axis=1,inplace=True)

            # Pivot
            df = df.pivot_table(index=["crop","Element"], columns=["year"], values="value", aggfunc=np.sum)

            df.reset_index(level=["crop","Element"], inplace=True)
            df.to_csv(final_file, index = False, encoding = self.encoding)

        else:
            print("\tNot processed: " + final_file)    
