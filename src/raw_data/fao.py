import os
import re
import glob

from pandas.core import groupby
import numpy as np
import pandas as pd

import tools.manage_files as mf
import tools.interdependence as id
import tools.gini as gi

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
    return df_merged

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
            merge_tables(final_path,f_name + ".csv",countries,df,"country","Area", encoding=encoding)        
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

# Method that merge files with the list of crops.
# It search by each file the item cleaned for getting the crop name
# (XLSParse) conf_crops: XLS Parse object which has the list of the crops
# (string) location: String with the path of where the system should take the files.
#                   It will filter all csv files from the path.
#                   It just will process the OK files
# (string) path: Location where the files should be saved
# (string) step: prefix of the output files. By default it is 03
# (string) encoding: Encoding files. By default it is ISO-8859-1
# (bool) force: Set if the process have to for the execution of all files even if the were processed before. 
#               By default it is False
def merge_crops(conf_crops,location,path,step="03",encoding="ISO-8859-1",force=False):        
    final_path = os.path.join(path,"fao",step)
    create_review_folders(final_path)
    crops = conf_crops.parse("crops")
    # Get
    files = glob.glob(os.path.join(location,'OK',"*.csv"))
    # Loop for all faostat files which where downloaded
    for full_name in files:
        f_name = full_name.rsplit(os.path.sep, 1)[-1]
        f_name = os.path.splitext(f_name)[0]
        # It checks if files should be force to process again or if the path exist
        if force or not os.path.exists(os.path.join(final_path,"OK",f_name + ".csv")):
            # Merge crops
            print("\tWorking with: " + full_name)            
            df = pd.read_csv(full_name, encoding = encoding)            
            crops_tmp = crops[["crop",f_name]]
            merge_tables(final_path,f_name + ".csv",crops_tmp,df,f_name,"Item_cleaned", encoding=encoding)        
        else:
            print("\tNot processed: " + full_name)
    
    print("\tCreating summary with Wrong Crops")
    summary_errors(final_path,"WrongCrops.csv","Item_cleaned",encoding=encoding)

# Method which sum all items by area and element.
# The values are sum throught years.
# The field key to sum values is Item_cleaned.
# It filters records with items useable
# (XLSParse) conf_crops: XLS Parse object which has the list of the crops
# (string) location: String with the path of where the system should take the files.
#                   It will filter all csv files from the path.
#                   It just will process the OK files
# (string) path: Location where the files should be saved
# (int[]) years: Array of ints with the years which will sum
# (string) step: prefix of the output files. By default it is 04
# (string) encoding: Encoding files. By default it is ISO-8859-1
# (bool) force: Set if the process have to for the execution of all files even if the were processed before. 
#               By default it is False
def sum_items(conf_crops,location,path,years,step="04",encoding="ISO-8859-1",force=False):    
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
            countries = df[["country","iso2"]].drop_duplicates()
            df = df.groupby(["crop","Item_cleaned","country","Element Code","Element"], as_index=False)[y_years].sum()
            df = pd.merge(countries,df,how='inner', on='country')
            df.to_csv(os.path.join(final_path,"OK",f_name + ".csv"), index = False, encoding = encoding)
        else:
            print("\tNot processed: " + full_name)


# Method which creates files for commodities.
# This files are needed in terms to define a factor of values
# for setting weights for specif crops. 
# The main output is the file commodities which is in folder SM
# (XLSParse) conf_crops: XLS Parse object which has the list of the crops
# (string) location: String with the path of where the system should take the files.
#                   It will filter all csv files from the path.
#                   It just will process the OK files
# (string) path: Location where the files should be saved
# (int[]) years: Array of ints with the years which will be sum
# (string) prod_file: Name of the production file. It shouldn't have the extension.
# (string) prod_field: Name of the element which the system should calculate weights. For example Production or Yield
# (string) step: prefix of the output files. By default it is 05
# (string) encoding: Encoding files. By default it is ISO-8859-1
# (bool) force: Set if the process have to for the execution of all files even if the were processed before. 
#               By default it is False
def calculate_commodities(conf_crops,location,path,years,prod_file,prod_field,step="05",encoding="ISO-8859-1",force=False):    
    final_path = os.path.join(path,"fao",step)    
    create_review_folders(final_path)
    path_commodities = os.path.join(final_path,"SM","commodities.csv")
    # Check if process should be execute
    if force or not os.path.exists(path_commodities):
        print("\tLoading items cleaned list and crops")
        crops = conf_crops.parse("crops")
        item_cleaned = conf_crops.parse("fao")
        fao_groups = conf_crops.parse("fao_groups")
        # Extracting groups and commodities
        df_commodities = item_cleaned.loc[(item_cleaned["Item_cleaned"].str.contains(" nes", flags=re.IGNORECASE))  
                                #| (item_cleaned["Item_cleaned"].str.contains(" other", flags=re.IGNORECASE))) & 
                                & (item_cleaned["useable"] == "Y"),:]
        # Setting which is the group
        df_commodities["nes"] = df_commodities["Item_cleaned"].apply(lambda x: 1 if x.lower().endswith(" nes") else 0)
        # Extracting the files 
        files = df_commodities["source"].drop_duplicates()    
        # Keep just the item cleaned and create columns for each file
        df_commodities = df_commodities[["Item_cleaned","nes"]].drop_duplicates(subset=['Item_cleaned'], keep='last')
        df_commodities = pd.concat([df_commodities,pd.DataFrame(columns=files)])
        # Loop to search how many crops contain each subgroup (nes)
        for f in files:
            print("\tSearching: ", f)        
            df_commodities[f] = df_commodities["Item_cleaned"].apply(lambda x: crops.loc[crops[f].str.contains(x, na=False),:].shape[0])
        print("\tSaving commodities")
        df_commodities.to_csv(os.path.join(final_path,"SM", "nes.csv"), index = False, encoding = encoding)

        # Reading production
        full_prod = os.path.join(location,"OK",prod_file + ".csv")
        print("\tReading: ", full_prod)
        df_prod = pd.read_csv(full_prod, encoding = encoding)
        # Merging with fao groups
        df_prod = merge_tables(final_path,prod_file + ".csv",df_prod,fao_groups,"Item_cleaned","Item_cleaned", encoding=encoding)
        # Calculating total for all years
        y_years = ["Y" + str(x) for x in years]
        df_prod[prod_field] = df_prod[y_years].sum(axis=1)
        # Filtering just by World and Production
        df_prod = df_prod.loc[(df_prod["country"] == "World") & (df_prod["Element"] == prod_field)]        
        # Sum rows by items
        df_prod = df_prod[["group","Item_cleaned","crop",prod_field]]        
        df_prod = df_prod.groupby(["group","Item_cleaned","crop"], as_index=False)[[prod_field]].sum()
        
        # Merge with commodities and set normal value
        df_prod = pd.merge(df_prod,df_commodities[["Item_cleaned","nes",prod_file]],how="left",on="Item_cleaned")
        df_prod[prod_field] = df_prod.apply(lambda x: x[prod_field] if pd.isnull(x['nes']) or x["nes"] == 0 else x[prod_field] / x[prod_file],axis=1)
        
        # Calculating total for group
        df_group = df_prod.groupby(["group"], as_index=False)[[prod_field]].sum()
        df_group.columns = ["group","total"]
        
        df_merged = pd.merge(df_prod,df_group,how="inner",left_on="group",right_on="group")
        
        df_merged["partial"] = df_merged[prod_field] / df_merged["total"]
        df_merged["percentage"] = df_merged["partial"]        
        df_merged.to_csv(path_commodities, index = False, encoding = encoding)
    else:
        print("\tNot processed: Commodities weren't calculated")
    
    return path_commodities

# Method that calculates the final values for all variables.
# It merges files with the commodities files, in terms to get the percentage that each crop contribute. 
# If a crop is not a commodities, the value will be the original
# (string) location: String with the path of where the system should take the files.
#                   It will filter all csv files from the path.
#                   It just will process the OK files
# (string) path: Location where the files should be saved
# (int[]) years: Array of ints with the years which will be sum
# (string[]) special_files: List of files.
# (commodities_file) path_commodities: Location where the commodities with the percentage is stored
# (string) step: prefix of the output files. By default it is 06
# (string) encoding: Encoding files. By default it is ISO-8859-1
# (bool) force: Set if the process have to for the execution of all files even if the were processed before. 
#               By default it is False
def calculate_values(location,path,years,special_files,commodities_file,step="06",encoding="ISO-8859-1",force=False):        
    final_path = os.path.join(path,"fao",step)
    create_review_folders(final_path,er=False,sm=False)
    # Get files to process
    files = glob.glob(os.path.join(location,'OK',"*.csv"))
    # Load commodities
    commodities = pd.read_csv(commodities_file, encoding = encoding) 
    commodities = commodities[["Item_cleaned","percentage","nes","group","crop"]]
    # Create a list of years
    y_years = ["Y" + str(x) for x in years]
    # Loop for all faostat files which where downloaded
    for full_name in files:
        f_name = full_name.rsplit(os.path.sep, 1)[-1]
        f_name = os.path.splitext(f_name)[0]
        # It checks if files should be force to process again or if the path exist
        if force or not os.path.exists(os.path.join(final_path,"OK",f_name + ".csv")):
            # Merge with commodities
            print("\tWorking with: " + full_name)            
            df = pd.read_csv(full_name, encoding = encoding)                                                
            # It is a condition for Production file, in this case we just change the values
            # for big groups i.e. (Cereals, nes) but not for small groups i.e (almonds)            
            if f_name not in special_files:
                df = pd.merge(commodities,df,how='right',on=["Item_cleaned","crop"])
                df.loc[df["nes"].isna(),"percentage"]=1
            else:
                df = pd.merge(commodities,df,how='right',left_on=["group","crop"],right_on=["Item_cleaned","crop"])
                            
            # If item cleaned is not a group, so the percentage will be null
            # we should set 1 to all crops which are not a group
            df.loc[df["percentage"].isna(),"percentage"]=1

            # Calculating final values for years
            print("\tCalculating final values for years ", y_years)
            for y in y_years:
                #df[y + "_new"] = df["percentage"]*df[y]
                df[y] = df["percentage"]*df[y]
            # Saving outputs
            df.to_csv(os.path.join(final_path,"OK",f_name + ".csv"), index = False, encoding = encoding)            
        else:
            print("\tNot processed: " + full_name)

# Method that calculates contribution of crops in each country by element.
# (string) location: String with the path of where the system should take the files.
#                   It will filter all csv files from the path.
#                   It just will process the OK files
# (string) path: Location where the files should be saved
# (int[]) years: Array of ints with the years which will be sum
# (string) step: prefix of the output files. By default it is 07
# (string) encoding: Encoding files. By default it is ISO-8859-1
# (bool) force: Set if the process have to for the execution of all files even if the were processed before. 
#               By default it is False
def calculate_contribution_crop_country(location,path,years,step="07",encoding="ISO-8859-1",force=False):        
    final_path = os.path.join(path,"fao",step)
    create_review_folders(final_path,er=False,sm=False)
    # Get files to process
    files = glob.glob(os.path.join(location,'OK',"*.csv"))
    # Create a list of years
    y_years = ["Y" + str(x) for x in years]
    # Loop for all faostat files which where downloaded
    for full_name in files:
        f_name = full_name.rsplit(os.path.sep, 1)[-1]
        f_name = os.path.splitext(f_name)[0]
        # It checks if files should be force to process again or if the path exist
        if force or not os.path.exists(os.path.join(final_path,"OK",f_name + ".csv")):
            # Getting from source
            print("\tWorking with: " + full_name)            
            df = pd.read_csv(full_name, encoding = encoding)
            # Filtering just data for countries
            df = df.loc[~df["iso2"].isna(),:]

            # Summarizing by country, element
            print("\tSummarizing by country and element")
            total = df.groupby(["country","Element Code","Element"], as_index=False)[y_years].sum()
            print("\tMerging with file")
            df = pd.merge(total,df,how='inner',on=["country","Element Code","Element"])
            print("\tLooping for calculating contribution by year")
            for y in y_years:
                print("\t\tYear: ",y)
                df[y + "_contrib"] = df[y+"_y"] / df[y+"_x"]
                df = df.sort_values(by=["country","Element", y+ "_contrib"], ascending=False)
                df[y + "_cumsum"] = df.groupby(["country","Element"])[[y + "_contrib"]].cumsum()
            
            # Saving outputs
            print("\tSaving output")
            df.to_csv(os.path.join(final_path,"OK",f_name + ".csv"), index = False, encoding = encoding)
        else:
            print("\tNot processed: " + full_name)

# Method that counts how many countries have crops.
# (string) location: String with the path of where the system should take the files.
#                   It will filter all csv files from the path.
#                   It just will process the OK files
# (string) path: Location where the files should be saved
# (int[]) years: Array of ints with the years which will be sum
# (double) limit: limit which establishes the importance of crops. By default it is 0.95
# (string) suffix: suffix field which has the percentage of each crop in a country
# (string) step: prefix of the output files. By default it is 08
# (string) encoding: Encoding files. By default it is ISO-8859-1
# (bool) force: Set if the process have to for the execution of all files even if the were processed before. 
#               By default it is False
def count_countries(location,path,years,limit=0.95,suffix="_cumsum",step="08",encoding="ISO-8859-1",force=False):        
    final_path = os.path.join(path,"fao",step)
    create_review_folders(final_path,er=False,sm=False)
    # Get files to process
    files = glob.glob(os.path.join(location,'OK',"*.csv"))
    # Loop for all faostat files which where downloaded
    for full_name in files:
        f_name = full_name.rsplit(os.path.sep, 1)[-1]
        f_name = os.path.splitext(f_name)[0]
        # It checks if files should be force to process again or if the path exist
        if force or not os.path.exists(os.path.join(final_path,"OK",f_name + ".csv")):            
            print("\tWorking with: " + full_name)
            df = pd.read_csv(full_name, encoding = encoding)
            # Getting a list of all crops into a dataframe
            df_crops = df[["crop","Element"]].drop_duplicates()            
            for y in years:
                print("\t\tCounting ",y)
                y_year = "Y" + str(y) + suffix
                df_tmp = df.loc[df[y_year] < limit, ["crop","Element", y_year]]
                df_tmp = df_tmp.dropna()
                df_tmp = df_tmp.groupby(["crop","Element"]).size().reset_index(name=str(y))
                df_crops = pd.merge(df_crops,df_tmp,how='left',on=["crop","Element"])
            
            print("\tSaving output")
            df_crops = df_crops.dropna(thresh=4)
            df_crops = df_crops.fillna(0)
            df_crops.to_csv(os.path.join(final_path,"OK",f_name + ".csv"), index = False, encoding = encoding)
        else:
            print("\tNot processed: " + full_name)         

# Method that calculates the amount of population for countries and regions.
# It returns the path where the files are located.
# The ouputs files
# (XLSParse) conf_countries: XLS Parse object which has the configurations for countries
# (string[]) files: Array which contains the folders locations of fao data. 
#                   It doesn't have location of specific files, 
#                   because it will search the file which has the same name of the folder
# (string) path: Location where the files should be saved
# (int[]) years: Array of ints with the years which will be sum
# (string) element_population: Name of element which has the population values
# (string) step: prefix of the output files. By default it is 09
# (string) encoding: Encoding files. By default it is ISO-8859-1
# (bool) force: Set if the process have to for the execution of all files even if the were processed before. 
#               By default it is False
def calculate_population(conf_countries,files,path,years,element_population,step="09",encoding="ISO-8859-1",force=False):
    final_path = os.path.join(path,"fao",step)
    create_review_folders(final_path)
    y_years = ["Y" + str(x) for x in years]
    # Get files to process
    for f in files:
        f_name = f.rsplit(os.path.sep, 1)[-1]
        full_name = os.path.join(f,f_name + ".csv")
        # It checks if files should be force to process again or if the path exist
        if force or not os.path.exists(os.path.join(final_path,"OK","population_countries.csv")):
            print("\tWorking with: " + full_name)
            df = pd.read_csv(full_name, encoding = encoding)
            # Filtering
            print("\t\tFiltering dataset by: ", element_population)
            df = df.loc[df["Element"] == element_population,:]

            # Getting regions
            print("\t\tMerging by country and Area")
            regions = conf_countries.parse('regions')
            df_merged = pd.merge(regions,df[["Area"] + y_years], how='left',left_on=['country'],right_on=['Area'])
            df_not_merged = df.loc[~df["Area"].isin(regions['country']) ,:]
            
            # Save outputs    
            print("\t\tSaving outputs.")
            f_countries = os.path.join(final_path,"OK","population_countries.csv")
            df_merged.to_csv(f_countries, index = False, encoding = encoding)
            df_not_merged.to_csv(os.path.join(final_path,"ER","population_countries_not_merged.csv"), index = False, encoding = encoding)

            total_regions = df_merged.groupby(['region'])[y_years].sum()            
            f_regions = os.path.join(final_path,"SM","population_region.csv")
            total_regions.to_csv(f_regions, index = True, encoding = encoding)

            df_final = pd.merge(total_regions,df_merged, how='inner',on=['region'])
            col_names = []
            for y in y_years:
                col_names.append(y)
                df_final[y] = df_final[y + "_y"] / df_final[y + "_x"]
            
            df_final = df_final[["region","country","iso2","iso3"] + col_names]
            f_segregated = os.path.join(final_path,"SM","population_segregated.csv")
            df_final.to_csv(f_segregated, index = False, encoding = encoding)
            print("\t\tOutputs saved.")


# Method that calculates the interdependence. It returns the path where OK files are located
# (XLSParse) conf_crops: XLS Parse object which has the configurations for crops
# (string) location: String with the path of where the system should take the files.
#                   It will filter all csv files from the path.
#                   It just will process the OK files
# (string) path: Location where the files should be saved
# (int[]) years: Array of ints with the years which will be sum
# (string[]) special_files: List of files. These files will be processed with proportion method
# (string) population: Path where the population files are. 
#                   It should have the three files: population by country, region and segregated
# (string) step: prefix of the output files. By default it is 10
# (string) encoding: Encoding files. By default it is ISO-8859-1
# (bool) force: Set if the process have to for the execution of all files even if the were processed before. 
#               By default it is False
def calculate_interdependence(conf_crops, location, path, years, special_files, population, step="10",encoding="ISO-8859-1",force=False):
    final_path = os.path.join(path,"fao",step)
    create_review_folders(final_path)
    y_years = ["Y" + str(x) for x in years]
    region_crops = conf_crops.parse("regions")
    
    # Loading the population files
    #df_population_countries = pd.read_csv(os.path.join(population,"OK","population_countries.csv"), encoding = encoding)
    df_population_region = pd.read_csv(os.path.join(population,"SM","population_region.csv"), encoding = encoding)
    df_population_segregated = pd.read_csv(os.path.join(population,"SM","population_segregated.csv"), encoding = encoding)
    
    # Get files to process
    files = glob.glob(os.path.join(location,'OK',"*.csv"))
    for full_name in files:
        f_name = full_name.rsplit(os.path.sep, 1)[-1]
        f_name = os.path.splitext(f_name)[0]
        final_file = os.path.join(final_path,"SM",f_name + ".csv")
        # It checks if files should be force to process again or if the path exist        
        if force or not os.path.exists(final_file):                    
            method = "proportion" if f_name in special_files else "sum"

            print("\tWorking with:",full_name,"Method:",method)
            df = pd.read_csv(full_name, encoding = encoding)   
            
            df_inter = id.interdependence(df,region_crops,method,y_years,final_path,f_name,df_population_region,df_population_segregated)
            print("\tSaving output")
            df_inter.to_csv(final_file, index = False, encoding = encoding)
    return os.path.join(final_path,"OK")

# Method that calculates the gini indicator to raw data
# (XLSParse) conf_countries: XLS Parse object which has the configurations for countries
# (string) location: String with the path of where the system should take the files.
#                   It will filter all csv files from the path.
#                   It just will process the OK files
# (string) path: Location where the files should be saved
# (int[]) years: Array of ints with the years which will be sum
# (string) population: Path where the population files are. 
#                   It should have the three files: population by country, region and segregated
# (string) step: prefix of the output files. By default it is 10
# (string) encoding: Encoding files. By default it is ISO-8859-1
# (bool) force: Set if the process have to for the execution of all files even if the were processed before. 
#               By default it is False
def calculate_gini(conf_countries, location, path, years, step="11",encoding="ISO-8859-1",force=False):
    final_path = os.path.join(path,"fao",step)
    create_review_folders(final_path, er=False, sm=False)
    y_years = ["Y" + str(x) for x in years]
    # Calculate the total regions to fill the dataset with zeros in the regions which the crop is not
    regions = conf_countries.parse("regions")
    regions_total = len(regions["region"].unique())
    # Get files to process
    folders = glob.glob(os.path.join(location,'OK', "*/"))
    for folder in folders:
        f_name = folder.rsplit(os.path.sep)[-2]
        final_file = os.path.join(final_path,"OK",f_name + ".csv")
        # It checks if files should be force to process again or if the path exist        
        if force or not os.path.exists(final_file):

            print("\tWorking with:",folder)
            df = pd.read_csv(os.path.join(folder,"01-summary_element_crop_region.csv"), encoding = encoding)   
            df_gini = gi.gini_crop(df,y_years,regions_total)

            print("\tSaving output")
            df_gini.to_csv(final_file, index = False, encoding = encoding)

