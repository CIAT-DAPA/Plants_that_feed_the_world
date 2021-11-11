import os
import pandas as pd

import tools.manage_files as mf

# Method which calculates interdependence of crops
# (dataframe) data: Dataframe with crop information
# (dataframe) crop_regions: Dataframe with the information about regions which crops are native
# (string) method: Define what should be the method to calculate interdependence.
#                   The values are sum or segregated
# (int[]) years: List of year that should be calculated
# (string) path: Path where the outputs should be save
# (string) folder: Folder name to save information by file
# (dataframe) pop_regions: Dataframe with the information about amount people by region
# (dataframe) pop_segregated: Dataframe with the information about
# (string) encoding: Encoding files. By default it is ISO-8859-1
def interdependence(data,crop_regions,method,years,path,folder, pop_regions, pop_segregated,encoding="ISO-8859-1"):
    final_folder = os.path.join(path,"OK",folder)
    mf.mkdir(final_folder)
    # Joining with population data
    tmp = pd.merge(data,pop_segregated,on="country",how="inner")

    # Validate if the final values are proportioned by population
    if method == "proportion":
        for y in years:
            # This proportion by country. 
            tmp[y + "_x"] = tmp[y + "_x"] * tmp[y + "_y"]
    
    # Fixing the column names
    cols_names = ["crop","Element", "region"] + [y +"_x" for y in years]
    tmp = tmp[cols_names]
    tmp.columns = ["crop","Element", "region"] + years

    # Calculate sum of crops by Element, crop and region
    tmp_summary = tmp.groupby(["Element","crop","region"], as_index=False)[years].sum()
    tmp_summary.to_csv(os.path.join(final_folder,"01-summary_element_crop_region.csv"), index = False, encoding = encoding)
    
    tmp_interdependence = pd.DataFrame()
    df_checked_in = pd.DataFrame()
    df_checked_out = pd.DataFrame()

    # Loop which calculates interdependence by each crop
    for crop in tmp["crop"].unique():
        # Getting native regions for by crop
        c_regions = crop_regions.loc[crop_regions["crop"] == crop,:]["region"].unique()
        df_crop = tmp_summary.loc[(tmp_summary["crop"] == crop), :]

        # Filtering inside and outside data
        df_inside = df_crop.loc[df_crop["region"].isin(c_regions),:]
        df_outside = df_crop.loc[~df_crop["region"].isin(c_regions),:]
        
        # Calculate population for proportion mode
        pop_inside = pop_regions.loc[(pop_regions["region"].isin(c_regions)), :][years].sum()
        pop_outside = pop_regions.loc[(~pop_regions["region"].isin(c_regions)), :][years].sum()
        pop_world = pop_inside + pop_outside

        # This section calculate production values for proportion mode
        pop_inside_seg = pd.DataFrame()
        pop_outside_seg = pd.DataFrame()
        if method == "proportion":
            # Filter population by region regarding to inside or outside
            pop_inside_seg = pop_regions.loc[(pop_regions["region"].isin(c_regions)), :]
            pop_outside_seg = pop_regions.loc[(~pop_regions["region"].isin(c_regions)), :]
            # Calculates the proportion of population by region
            for y in years:
                # This is proportion by regions inside and outside
                pop_inside_seg[y] = pop_inside_seg[y] / pop_inside[y]
                pop_outside_seg[y] = pop_outside_seg[y] / pop_outside[y]

            # Merging population with production
            tmp_inside = pd.merge(df_inside,pop_inside_seg,on="region",how="inner")
            tmp_outside = pd.merge(df_outside,pop_outside_seg,on="region",how="inner")
            # Loop for applying proportion in the columns
            for y in years:
                tmp_inside[y] = tmp_inside[y + "_x"] * tmp_inside[y + "_y"]
                tmp_outside[y] = tmp_outside[y + "_x"] * tmp_outside[y + "_y"]

            # Fix the dataframe which will be processed
            cols_names = ["Element","crop", "region"] + years
            df_inside = tmp_inside[cols_names]
            df_outside = tmp_outside[cols_names]

        # Adding information to files which will be used to check output
        df_checked_in = df_checked_in.append(df_inside,ignore_index=True)
        df_checked_out = df_checked_out.append(df_outside,ignore_index=True)

        # Calculates total for variable and crop
        df_inside_total = df_inside.groupby(['Element','crop'],as_index=False)[years].sum()
        df_outside_total = df_outside.groupby(['Element','crop'],as_index=False)[years].sum()

        # Merge data for origin regions and outside regiones in one dataframe
        df_total = pd.merge(df_inside_total, df_outside_total, on=["Element","crop"], how="inner", suffixes=("_in","_out"))        
        # Loop for calculating interdependence by each year
        for y in years:
            if method == "proportion":
                df_total[y + "_world"] = (df_total[y + "_in"]*(pop_inside[y]/pop_world[y])) + (df_total[y + "_out"]*(pop_outside[y]/pop_world[y]))
            else:
                df_total[y + "_world"] = df_total[y + "_in"] + df_total[y + "_out"]

            df_total[y + "_global"] = df_total[y + "_out"] / df_total[y + "_world"]

            # Fixing the values for proportion which can be struggle            
            df_total.at[df_total[y + "_global"] > 1,y + "_global"] = 1
                
        tmp_interdependence = tmp_interdependence.append(df_total, ignore_index=True)
    
    print("\t\tSaving logs")
    df_checked = pd.merge(df_checked_in,df_checked_out,on=["Element","crop","region"],how="outer",suffixes=("_in","_out"))
    df_checked.to_csv(os.path.join(final_folder,"02-check.csv"), index = False, encoding = encoding)
    #df_checked_in.to_csv(os.path.join(final_folder,"02-in.csv"), index = False, encoding = encoding)
    #df_checked_out.to_csv(os.path.join(final_folder,"02-out.csv"), index = False, encoding = encoding)
    return tmp_interdependence

