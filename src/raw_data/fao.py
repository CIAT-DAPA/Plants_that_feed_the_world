import os
import pandas as pd
import glob

import tools.manage_files as mf

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
    mf.mkdir(final_path)
    # Loop for all faostat files which where downloaded
    for f in files:
        f_name = f.rsplit(os.path.sep, 1)[-1]
        full_name = os.path.join(f,f_name + ".csv")
        # It checks if files should be force to process again or if the path exist
        if force or not os.path.exists(full_name):
            # Merge countries
            print("\tWorking with: " + full_name)
            df = pd.read_csv(full_name, encoding = encoding)
            df_merged = pd.merge(countries,df,how="inner",left_on="name",right_on="Area")
            df_not_merged = df.loc[~df["Area"].isin(countries["name"]) ,:]            
            print("\tFile loaded. Original: " + str(df.shape[0]) + " Merged: " + str(df_merged.shape[0]) + " Not merged: " + str(df_not_merged.shape[0]))
            # Save outputs
            df_merged.to_csv(os.path.join(final_path,"OK-" + f_name + ".csv"), index = False, encoding = encoding)
            df_not_merged.to_csv(os.path.join(final_path,"ER-" + f_name + ".csv"), index = False, encoding = encoding)
        
        else:
            print("\tNot processed: " + full_name)
    
    print("\tCreating summary with Wrong countries")
    c_missing = []
    wr_files = glob.glob(os.path.join(final_path,'ER-*.csv'))
    for f in wr_files:
        df = pd.read_csv(f, encoding = encoding)
        c_missing.extend(df["Area"].drop_duplicates())
    
    c_missing = list(set(c_missing))
    df_missing = pd.DataFrame({'Area':c_missing})
    file_missing = os.path.join(final_path,"SM-WrongCountries.csv")
    print("\tWrong Countries: " + file_missing)
    df_missing.to_csv(file_missing, index = False, encoding = encoding)

# Method which merge the fao data with custom list of crops.
# It checks if the crops match between custom list and fao list.
# It creates logs about wrong matches
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
def merge_crops(conf_crops,location,path,step="02",encoding="ISO-8859-1",force=False):    
    print("\tLoading crops list")
    crops = conf_crops.parse("fao")
    final_path = os.path.join(path,"fao",step)    
    mf.mkdir(final_path)
    files = glob.glob(os.path.join(location,'OK-*.csv'))
    # Loop for all faostat files which where downloaded
    for f in files:
        f_name = f.rsplit(os.path.sep, 1)[-1]
        full_name = os.path.join(final_path,f_name + ".csv")
        # It checks if files should be force to process again or if the path exist
        if force or not os.path.exists(full_name):
            # Merge crops
            print("\tWorking with: " + f)
            df = pd.read_csv(f, encoding = encoding)            
            df_merged = pd.merge(crops,df,how="inner",left_on="name",right_on="Item")
            df_not_merged = df.loc[~df["Item"].isin(crops["name"]) ,:]            
            print("\tFile loaded. Original: " + str(df.shape[0]) + " Merged: " + str(df_merged.shape[0]) + " Not merged: " + str(df_not_merged.shape[0]))
            # Save outputs
            df_merged.to_csv(full_name, index = False, encoding = encoding)
            df_not_merged.to_csv(os.path.join(final_path,"ER-" + f_name + ".csv"), index = False, encoding = encoding)
        
        else:
            print("\tNot processed: " + full_name)
    
    print("\tCreating summary with Wrong crops")
    c_missing = []
    wr_files = glob.glob(os.path.join(final_path,'ER-*.csv'))
    for f in wr_files:
        df = pd.read_csv(f, encoding = encoding)
        c_missing.extend(df["Item"].drop_duplicates())
    
    c_missing = list(set(c_missing))
    df_missing = pd.DataFrame({'Item':c_missing})
    file_missing = os.path.join(final_path,"SM-WrongCrops.csv")
    print("\tWrong Crops: " + file_missing)
    df_missing.to_csv(file_missing, index = False, encoding = encoding)

    

    
    


    