import os
import re
import glob

from pandas.core import groupby
import numpy as np
import pandas as pd

import tools.manage_files as mf

class Google(object):
    
    encoding = ''
    year = 0

    ## Method construct
    def __init__(self, year = 2019, encoding="ISO-8859-1"):
        
        self.encoding = encoding
        self.year = year

    # Method that counts how many countries have crops.
    # (string) location: String with the path of where the system should take the XLS file.
    # (string) path: Location where the files should be saved    
    # (string) sheet: Sheet name where data is saved
    # (string) field_crop: Field name which has the crop name
    # (string[]) fields_elements: Array of strings which the columns names which have data (values)
    # (string) field_year: Field name which has the year
    # (string) step: prefix of the output files. By default it is 01
    # (bool) force: Set if the process have to for the execution of all files even if the were processed before. 
    #               By default it is False
    def fix_data(self, location, path, sheet, field_crop, fields_elements, field_year= "year",step="01",force = False):
        final_path = os.path.join(path,"google",step)
        mf.create_review_folders(final_path, er=False, sm=False)

        f_name = location.rsplit(os.path.sep, 1)[-1]
        f_name = os.path.splitext(f_name)[0]
        final_file = os.path.join(final_path,"OK","google.csv")
        # It checks if files should be force to process again or if the path exist
        if force or not os.path.exists(final_file):
            # Loading data
            print("\tLoading data:", location)
            data_xls = pd.ExcelFile(location)
            df = data_xls.parse(sheet)
            df_final = pd.DataFrame()            
            for e in fields_elements:
                print("\t\tFixing:", e)
                tmp = pd.DataFrame()
                tmp = df[[field_crop,e]]
                tmp.columns = ["crop","Y" + str(self.year)]
                tmp["Element"] = e
                if df_final.shape[0] <= 0:
                    df_final = tmp
                else:
                    df_final = df_final.append(tmp,ignore_index=True)
            df_fix = df_final[["crop", "Element","Y" + str(self.year)]]
            df_fix.to_csv(os.path.join(final_path,"OK","google.csv"), index = False, encoding = self.encoding)            
        else:
            print("\tNot processed: " + final_file)    
