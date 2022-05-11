from cmath import nan
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
    outputs_name=''
    encoding=''

    

    # Method construct
    def __init__(self,output_folder,outputs_name='indicator',encoding="ISO-8859-1"):
        self.output_folder = output_folder
        self.outputs_name = outputs_name
        self.encoding = encoding

    # Method that join all datasets in just one file, further it standarize the name of fields
    # and create and average through years. For the averga it just takes into account fields with data
    # fields without data won't take into account
    # (string) folder_raw: Folder where raw data is located
    # (dataframe) conf_indicator: Configuration for all metrics
    # (string) step: prefix of the output files. By default it is 01
    # (bool) force: Set if the process have to for the execution of all files even if the were processed before. 
    #               By default it is False
    def extract_raw_data(self, folder_raw, conf_indicator, years,step="01",force=False):
        final_path = os.path.join(self.output_folder,step)
        mf.create_review_folders(final_path,sm=False)
        final_file = os.path.join(final_path,"OK",self.outputs_name + ".csv")
        err_file = os.path.join(final_path,"ER",self.outputs_name + ".csv")
        # Validating if final file exist
        if force or not os.path.exists(final_file):
            # Create global dataframe
            df = pd.DataFrame()
            df_err = pd.DataFrame()
            # Loop for each configuration, it means foreach metrics
            for index, row in conf_indicator.iterrows():
                if not pd.isnull(row["element"]):
                    print("\tWorking with: ")
                    raw_file = os.path.join(folder_raw,row["source"],row["step"],row["folder"],row["file"])
                    print("\t\tRaw data",raw_file,row["source"],row["step"],row["folder"],row["file"],row["element"])
                    print("\t\tIndicator",row["domain"],row["component"],row["group"],row["metric"],row["prefix_year"])

                    # Filtering
                    df_element = pd.read_csv(raw_file, encoding = self.encoding)
                    rows_full = df_element.shape[0]
                    df_element = df_element.loc[df_element["Element"]==row["element"],:]
                    rows_filter = df_element.shape[0]
                    print("\t\tTotal records:",rows_full,"filtered records:",rows_filter)

                    df_tmp = df_element[["crop"]]
                    df_tmp["domain"] = row["domain"]
                    df_tmp["component"] = row["component"]
                    df_tmp["group"] = row["group"]
                    df_tmp["metric"] = row["metric"]

                    for y in years:
                        prefix = "" if pd.isnull(row["prefix_year"])  else row["prefix_year"]
                        y_name_real = prefix + str(y)
                        y_name_final = "Y" + str(y)
                        print("\t\t\tProcessing",y,row["prefix_year"],y_name_real,y_name_final)
                        # Check if the file contains values for this year
                        df_tmp[y_name_final] = df_element[y_name_real] if y_name_real in df_element.columns else np.nan
                    
                    all_years = ["Y" + str(y) for y in years]
                    # Calculate average
                    df_tmp["average"] = df_tmp[all_years].mean(axis=1,skipna=True)
                    if df.shape[0] > 0:
                        df = df.append(df_tmp,ignore_index=True)
                    else:
                        df = df_tmp

                else:
                    df_err = df_err.append({"domain":row["domain"],"component":row["component"],"group":row["group"],"metric":row["metric"],"prefix_year":row["prefix_year"]},ignore_index=True)
                    print("\t\tNot processed: ",row["domain"],row["component"],row["group"],row["metric"],row["prefix_year"])
            
            #  Saving output
            df.to_csv(final_file, index = False, encoding = self.encoding)
            df_err.to_csv(err_file, index = False, encoding = self.encoding)
        else:
            print("\tNot processed: " + final_file)
    

    # Method that checks that fixes all data from raw sources.
    # Some actions are check if crops are correct
    # (string) step: prefix of the output files. By default it is 01
    # (bool) force: Set if the process have to for the execution of all files even if the were processed before. 
    #               By default it is False
    def check_data(self, crops, new_names, step="02",force=True):
        final_path = os.path.join(self.output_folder,step)
        mf.create_review_folders(final_path,sm=False)
        final_file = os.path.join(final_path,"OK",self.outputs_name + ".csv")
        err_file_not_ready = os.path.join(final_path,"ER",self.outputs_name + "_not_ready.csv")
        err_file_not_updated = os.path.join(final_path,"ER",self.outputs_name + "_not_updated.csv")
        # Validating if final file exist
        if force or not os.path.exists(final_file):
            # Create global dataframe
            df = pd.read_csv(os.path.join(self.output_folder,"01","OK",self.outputs_name + ".csv"), encoding = self.encoding)
            df_not_ready = df.loc[~df["crop"].isin(crops["crop"]) ,:]
            
            # Updating names
            df = pd.merge(df,new_names,how='left',left_on="crop", right_on="old")            
            df.loc[~df["new"].isna(),"crop"] = df.loc[~df["new"].isna(),:]["new"]
            df.drop(["new","old"],axis=1,inplace=True)
            # Checking changes
            df_not_updated = df.loc[~df["crop"].isin(crops["crop"]) ,:]

            # Save outputs    
            df.to_csv(final_file, index = False, encoding = self.encoding)
            df_not_ready.to_csv(err_file_not_ready, index = False, encoding = self.encoding)
            df_not_updated.to_csv(err_file_not_updated, index = False, encoding = self.encoding)
            print("\t\tOutputs saved.")
        else:
            print("\tNot processed: " + final_file)

    # Method that checks that fixes all data from raw sources.
    # (string) step: prefix of the output files. By default it is 01
    # (bool) force: Set if the process have to for the execution of all files even if the were processed before. 
    #               By default it is False
    def arrange_format(self,  step="03",force=True):
        final_path = os.path.join(self.output_folder,step)
        mf.create_review_folders(final_path,er=False,sm=False)
        final_file = os.path.join(final_path,"OK",self.outputs_name + ".csv")
        # Validating if final file exist
        if force or not os.path.exists(final_file):
            # Create global dataframe
            df = pd.read_csv(os.path.join(self.output_folder,"02","OK",self.outputs_name + ".csv"), encoding = self.encoding)
            df["var"] = df["domain"] + "-" + df["component"] + "-" + df["group"] + "-" + df["metric"]
            df = df.pivot_table(index=["crop"], columns=["var"], values="average", aggfunc=np.sum)
            df.reset_index(level=["crop"], inplace=True)
            df.to_csv(final_file, index = False, encoding = self.encoding)
            print("\t\tOutputs saved.")
        else:
            print("\tNot processed: " + final_file)