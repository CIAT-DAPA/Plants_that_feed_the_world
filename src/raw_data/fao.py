import os
import re
import glob
import numpy as np
import pandas as pd

import tools.manage_files as mf

# Method which creates the folders OK, ER, SM for each step
# (string) path: Path where the folders should be create
# (bool) ok: Create folder OK
# (bool) er: Create folder ER
# (bool) sm: Create folder SM
def create_review_folders(path, ok=True, er=True, sm=True):
    mf.mkdir(path)
    if ok:
        mf.mkdir(os.path.join(path,"OK"))
    if er:
        mf.mkdir(os.path.join(path,"ER"))
    if sm:
        mf.mkdir(os.path.join(path,"SM"))

# Method which merge two dataframe, then stablishs which records from 
# right dataset are not in left dataset. 
# It saves the outputs good in OK folder and not found records in ER folder
# (string) path_step: Root path of the step where outputs should be saved
# (string) name: Output file name
# (dataframe) left: Left dataset
# (dataframe) right: Right dataset
# (string) left_on: Fields from left dataset to join the dataframes
# (string) right_on: Fields from right dataset to join the dataframes
# (string) how: Method to join both datasets
# (string) encoding: Encoding files. By default it is ISO-8859-1
def merge_tables(path_step,name,left,right,left_on,right_on,how="inner", encoding="ISO-8859-1"):
    df_merged = pd.merge(left,right,how=how,left_on=left_on,right_on=right_on)
    df_not_merged = right.loc[~right[right_on].isin(left[left_on]) ,:]            
    print("\t\tFile loaded. Original: " + str(right.shape[0]) + " Merged: " + str(df_merged.shape[0]) + " Not merged: " + str(df_not_merged.shape[0]))
    # Save outputs    
    df_merged.to_csv(os.path.join(path_step,"OK",name), index = False, encoding = encoding)
    df_not_merged.to_csv(os.path.join(path_step,"ER",name), index = False, encoding = encoding)
    print("\t\tOutputs saved.")

# Method which creates a summary of errors found in a step
# (string) path_step: Root path of the step where outputs should be saved
# (string) name: Output file name
# (string) field: List of field to extract from the errores files
# (bool) add_file: Add a new column with the name of the file which has the error
# (string) encoding: Encoding files. By default it is ISO-8859-1
def summary_errors(path_step,name,field,add_file=False,encoding="ISO-8859-1"):
    missing = []
    file = []
    wr_files = glob.glob(os.path.join(path_step,'ER',"*.csv"))
    for f in wr_files:
        df = pd.read_csv(f, encoding = encoding)
        records = df[field].drop_duplicates()
        missing.extend(records)
        file.extend([os.path.splitext(os.path.basename(f))[0]] * len(records))

    df_missing = pd.DataFrame()
    if add_file:
        df_missing = pd.DataFrame({field:missing,"file":file})
    else:
        df_missing = pd.DataFrame({field:missing})
    df_missing = df_missing.drop_duplicates()
    file_missing = os.path.join(path_step,"SM",name)
    print("\t\tSaving summary error: " + file_missing)
    df_missing.to_csv(file_missing, index = False, encoding = encoding)

# Method which creates a folder in which files of the process will be save
# (string) path: Location where the folder should be created
def create_workspace(path):
    mf.mkdir(os.path.join(path,"fao"))
    return True

# Method which merge the fao data with our list of countries.
# It checks if the countries match between custom list and fao list.
# It creates logs about wrong matches
# (dataframe) countries: List of countries
# (string[]) files: Array which contains the folders locations of fao data. 
#                   It doesn't have location of specific files, 
#                   because it will search the file which has the same name of the folder
# (string) path: Location where the files should be saved
# (string) step: prefix of the output files. By default it is 01
# (string) encoding: Encoding files. By default it is ISO-8859-1
# (bool) force: Set if the process have to for the execution of all files even if the were processed before. 
#               By default it is False
def merge_countries(countries,files,path,step="01",encoding="ISO-8859-1",force=False):    
    final_path = os.path.join(path,"fao",step)
    create_review_folders(final_path)
    # Loop for all faostat files which where downloaded
    for f in files:
        f_name = f.rsplit(os.path.sep, 1)[-1]
        full_name = os.path.join(f,f_name + ".csv")
        # It checks if files should be force to process again or if the path exist
        if force or not os.path.exists(os.path.join(final_path,"OK",f_name + ".csv")):
            # Merge countries
            print("\tWorking with: " + full_name)
            df = pd.read_csv(full_name, encoding = encoding)
            merge_tables(final_path,f_name + ".csv",countries,df,"name","Area", encoding=encoding)        
        else:
            print("\tNot processed: " + full_name)
    
    print("\tCreating summary with Wrong countries")
    summary_errors(final_path,"WrongCountries.csv","Area",encoding=encoding)

# Method which sets the item cleaned for the items list from fao.
# (XLSParse) conf_crops: XLS Parse object which has the list of the crops
# (string) location: String with the path of where the system should take the files.
#                   It will filter all csv files from the path.
#                   The files should have the country fixed.
#                   It just will process the OK files
# (string) path: Location where the files should be saved
# (string) step: prefix of the output files. By default it is 02
# (string) encoding: Encoding files. By default it is ISO-8859-1
# (bool) force: Set if the process have to for the execution of all files even if the were processed before. 
#               By default it is False
def item_cleaned(conf_crops,location,path,step="02",encoding="ISO-8859-1",force=False):    
    print("\tLoading items cleaned list")
    item_cleaned = conf_crops.parse("fao")
    final_path = os.path.join(path,"fao",step)    
    create_review_folders(final_path)
    files = glob.glob(os.path.join(location,'OK',"*.csv"))
    # Loop for all faostat files which are ok with previews steps
    for full_name in files:
        f_name = full_name.rsplit(os.path.sep, 1)[-1]
        f_name = os.path.splitext(f_name)[0]
        # It checks if files should be force to process again or if the path exist
        if force or not os.path.exists(os.path.join(final_path,"OK",f_name + ".csv")):
            # Merge crops
            print("\tWorking with: " + full_name)
            df = pd.read_csv(full_name, encoding = encoding)
            item_cleaned_file = item_cleaned.loc[item_cleaned["source"] == f_name,:]
            merge_tables(final_path,f_name + ".csv",item_cleaned_file,df,"Item","Item",encoding=encoding)
        else:
            print("\tNot processed: " + full_name)
    
    print("\tCreating summary with Wrong crops")
    summary_errors(final_path,"WrongCrops.csv","Item",add_file=True,encoding=encoding)

# Method which sum all items by area and element.
# The values are sum throught years.
# The field key to sum values is Item_cleaned
# (XLSParse) conf_crops: XLS Parse object which has the list of the crops
# (string) location: String with the path of where the system should take the files.
#                   It will filter all csv files from the path.
#                   The files should have the country fixed.
#                   It just will process the OK files
# (string) path: Location where the files should be saved
# (int[]) years: Array of ints with the years which will sum
# (string) step: prefix of the output files. By default it is 03
# (string) encoding: Encoding files. By default it is ISO-8859-1
# (bool) force: Set if the process have to for the execution of all files even if the were processed before. 
#               By default it is False
def sum_items(conf_crops,location,path,years,step="03",encoding="ISO-8859-1",force=False):    
    print("\tLoading items cleaned list and crops")    
    item_cleaned = conf_crops.parse("fao")
    final_path = os.path.join(path,"fao",step)    
    create_review_folders(final_path, er=False, sm=False)
    files = glob.glob(os.path.join(location,'OK',"*.csv"))
    # Transforming years
    y_years = ["Y" + str(x) for x in years]
    # Loop for all faostat files which are ok with previews steps
    for full_name in files:
        f_name = full_name.rsplit(os.path.sep, 1)[-1]
        f_name = os.path.splitext(f_name)[0]
        # It checks if files should be force to process again or if the path exist
        if force or not os.path.exists(os.path.join(final_path,"OK",f_name + ".csv")):            
            print("\tWorking with: " + full_name)
            df = pd.read_csv(full_name, encoding = encoding)
            # Filtering items useable
            df = df[df["useable"]=="Y"]
            # Grouping by Item cleaned, country and element
            df = df.groupby(["Item_cleaned","name","iso2","Element Code","Element"], as_index=False)[y_years].sum()            
            df.to_csv(os.path.join(final_path,"OK",f_name + ".csv"), index = False, encoding = encoding)
        else:
            print("\tNot processed: " + full_name)

def calculate_commodities(conf_crops,path,step="04",encoding="ISO-8859-1",force=False):    
    print("\tLoading items cleaned list and crops")
    crops = conf_crops.parse("crops")
    item_cleaned = conf_crops.parse("fao")
    final_path = os.path.join(path,"fao",step)    
    create_review_folders(final_path, ok=False, er=False)

    # Extracting groups and commodities
    df_nes_others = item_cleaned.loc[((item_cleaned["Item_cleaned"].str.contains(" nes", flags=re.IGNORECASE)) | 
                                    (item_cleaned["Item_cleaned"].str.contains(" other", flags=re.IGNORECASE))) & 
                                    (item_cleaned["useable"] == "Y"),:]
    # Setting which is the group
    df_nes_others["nes"] = df_nes_others["Item_cleaned"].apply(lambda x: 1 if x.lower().endswith(" nes") else 0)
    df_nes_others["other"] = df_nes_others["Item_cleaned"].apply(lambda x: 1 if x.lower().endswith(" other") else 0)
    # Extracting the files 
    files = df_nes_others["source"].drop_duplicates()    
    # Keep just the item cleaned and create columns for each file
    df_nes_others = df_nes_others[["Item_cleaned","nes","other"]].drop_duplicates(subset=['Item_cleaned'], keep='last')
    df_nes_others = pd.concat([df_nes_others,pd.DataFrame(columns=files)])
    for f in files:
        print("\tSearching: ", f)        
        df_nes_others[f] = df_nes_others["Item_cleaned"].apply(lambda x: crops.loc[crops[f].str.contains(x, na=False),:].shape[0])

    df_nes_others.to_csv(os.path.join(final_path,"SM", "commodities.csv"), index = False, encoding = encoding)
    

    
    


    