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
    def extract_raw_data(self, folder_raw, conf_indicator, years, crops_list,step="01",force=False):
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
                    # Calculate filling crops just for specific domain
                    if row["domain"] != "crop_use" and row["domain"] != "interdependence":
                        df_crops = crops_list["crop"]
                        df_tmp = pd.merge(df_crops,df_tmp,how="left",on=["crop"])
                        df_tmp["domain"] = df_tmp["domain"].fillna(row["domain"])
                        df_tmp["component"] = df_tmp["component"].fillna(row["component"])
                        df_tmp["group"] = df_tmp["group"].fillna(row["group"])
                        df_tmp["metric"] = df_tmp["metric"].fillna(row["metric"])
                    # Calculate average
                    df_tmp["average"] = df_tmp[all_years].mean(axis=1,skipna=True)
                    if row["domain"] != "crop_use" and row["domain"] != "interdependence":
                        df_tmp["average"] = df_tmp["average"].fillna(0)
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


    # Method that calculate indicator, further it checks that fixes all data from raw sources.
    # Some actions are check if crops are correct
    # (dataframe) conf_indicator: Configuration for all metrics
    # (string) step: prefix of the output files. By default it is 01
    # (bool) force: Set if the process have to for the execution of all files even if the were processed before. 
    #               By default it is False
    def calculate_indicator(self, crops, new_names, conf_indicator, step="02",force=False):
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

            # Remove records with NA's
            df = df[df['average'].notna()]

            df = self.create_indicator(df,conf_indicator)

            # Save outputs
            df.to_csv(final_file, index = False, encoding = self.encoding)
            df_not_ready.to_csv(err_file_not_ready, index = False, encoding = self.encoding)
            df_not_updated.to_csv(err_file_not_updated, index = False, encoding = self.encoding)
            print("\t\tOutputs saved.")
        else:
            print("\tNot processed: " + final_file)
        return final_file
    
    # Method that creates indicator base on average
    # (dataframe) df_values: Dataframe with indicator values
    # (dataframe) conf_indicator: Dataframe with configuration to calculate indicators
    # return dataframe with indicators calculate
    def create_indicator(self,df_values, conf_indicator):
        df = df_values
        # Remove records with NA's
        df = df[df['average'].notna()]

        # Calculate indicators
        # Loop for each configuration, it means foreach metrics
        df["indicator"] = np.nan
        df["normalized"] = np.nan
        print("\tCalculating indicator")
        for index, row in conf_indicator.iterrows():
            print("\t\tIndicator",row["domain"],row["component"],row["group"],row["metric"],row["indicator_method"])
            # Filter records
            rows_selected = (df["domain"] == row["domain"]) & (df["component"] == row["component"]) & (df["group"] == row["group"]) &  (df["metric"] == row["metric"])
            # This method sum all records of the metric and divide each crop by the total
            if not pd.isnull(row["indicator_method"]) and row["indicator_method"] == "across_crops":
                value = df.loc[rows_selected,"average"].sum()
                df.loc[rows_selected,"indicator"]= df.loc[rows_selected,"average"] / value
            # This method divide value by setted value
            elif not pd.isnull(row["indicator_method"]) and row["indicator_method"] == "by_value":
                value = row["indicator_value"]
                df.loc[rows_selected,"indicator"]= df.loc[rows_selected,"average"] / value
            # This method sum all records of the metric and divide each crop by the total
            elif not pd.isnull(row["indicator_method"]) and row["indicator_method"] == "element_per_crop":
                metric = row["indicator_value"].split(",")
                rows_selected2 = (df["domain"] == metric[0]) & (df["component"] == metric[1]) & (df["group"] == metric[2]) &  (df["metric"] == metric[3])
                df_denominator = df.loc[rows_selected2,:]
                df.loc[rows_selected,"indicator"]= df.loc[rows_selected,:].apply(lambda x: self.element_per_crop(x["crop"],x["average"],df_denominator), axis=1)
            # This method checks that value of indicator does not exceed the limit
            elif not pd.isnull(row["indicator_method"]) and row["indicator_method"] == "limit":
                value = row["indicator_value"]
                df.loc[rows_selected,"indicator"]= df.loc[rows_selected,:].apply(lambda x: self.limit(x["average"],value), axis=1)
            else:
                df.loc[rows_selected,"indicator"]= df.loc[rows_selected,"average"]
            # Normalization
            max = df.loc[rows_selected,"indicator"].max()
            min = df.loc[rows_selected,"indicator"].min()
            if min != max:
                df.loc[rows_selected,"normalized"] = (df.loc[rows_selected,"indicator"] - min) / (max-min)
            else:
                df.loc[rows_selected,"normalized"] = 1
        return df

    # Method which calculates the indicator with the methodology element per crop
    # It search the value for each crop in other source and divide the current value with
    # the found.
    # (string) crop: Crop name
    # (double) value: Crop's value
    # (dataframe) source: Dataframe with the values' list for denominator
    # return value of indicator for the crop
    def element_per_crop(self,crop,value,source):
        answer = np.nan
        denominator = source.loc[source["crop"] == crop,"average"]
        if len(denominator.values) > 0:
            answer = value / denominator.values[0]
            #print(crop, value, denominator.values[0],answer)
        return answer

    # Method that checks if value is higher than limit
    # if value is higher, it sets the limit value, otherwhise it just return the current value
    # (float) value: Value to check
    # (float) limit: Limit for the indicator
    def limit(self,value,limit):
        answer = limit if float(value) > float(limit) else value
        return answer


    # Method that checks that fixes all data from raw sources.
    # (string[]) metrics: array of metrics that want to export
    # (string) step: prefix of the output files. By default it is 01
    # (bool) force: Set if the process have to for the execution of all files even if the were processed before. 
    #               By default it is False
    def arrange_format(self, metrics=["average","indicator","normalized"], step="03",force=True):
        final_path = os.path.join(self.output_folder,step)
        mf.create_review_folders(final_path,er=False,sm=False)
        final_path = os.path.join(final_path,'OK')
        files = glob.glob(os.path.join(final_path,"*.csv"))
        # Validating if final file exist
        if force or len(files) == 0: #not os.path.exists(final_file):
            # Create global dataframe
            df = pd.read_csv(os.path.join(self.output_folder,"02","OK",self.outputs_name + ".csv"), encoding = self.encoding)
            self.format_df_indicator(df,metrics,final_path)
        else:
            print("\tNot processed: " + final_path)
        return final_path

    # Method that generates outputs for indicator
    def format_df_indicator(self,df, metrics, final_path):
        df["var"] = df["domain"] + "-" + df["component"] + "-" + df["group"] + "-" + df["metric"]
        df["idx_group"] = df["domain"] + "-" + df["component"] + "-" + df["group"]
        df["idx_component"] = df["domain"] + "-" + df["component"]

        for m in metrics:
            df_m = df.pivot_table(index=["crop"], columns=["var"], values=m, aggfunc=np.sum)
            df_m.reset_index(level=["crop"], inplace=True)

            # Calculate mean by group
            print("\t\t\tCalculating mean by group")
            idx_g = df["idx_group"].drop_duplicates()
            for index, value in idx_g.items():
                print("\t\t\t\t",value)
                cols =  [col for col in df_m if col.startswith(value)]
                df_m["idx-group-" + value] = df_m[cols].mean(axis=1)

            # Calculate mean by group
            print("\t\t\tCalculating mean by component")
            idx_c = df["idx_component"].drop_duplicates()
            for index, value in idx_c.items():
                print("\t\t\t\t",value)
                cols =  [col for col in df_m if col.startswith("idx-group-" + value)]
                df_m["idx-component-" +value] = df_m[cols].mean(axis=1)
                
            # Calculate mean by group
            print("\t\t\tCalculating mean by domain")
            idx_d = df["domain"].drop_duplicates()
            for index, value in idx_d.items():
                print("\t\t\t\t",value)
                cols =  [col for col in df_m if col.startswith("idx-component-" + value)]
                df_m["idx-domain-" +value] = df_m[cols].mean(axis=1)
                
            # Calculate mean by group
            print("\t\t\tCalculating mean by crop")
            cols =  [col for col in df_m if col.startswith("idx-domain")]
            df_m["idx-crop"] = df_m[cols].mean(axis=1)
                
            final_file = os.path.join(final_path,self.outputs_name + "_" + m + ".csv")
            df_m.to_csv(final_file, index = False, encoding = self.encoding)
        print("\t\tOutputs saved.")

    # Method that calculates indicator by groups
    # (string) location: path of input file
    # (string[]) metrics: array of metrics that want to export
    # (string) step: prefix of the output files. By default it is 04
    # (bool) force: Set if the process have to for the execution of all files even if the were processed before. 
    #               By default it is False
    def calculate_indicator_by_use(self, new_names, conf_indicator, df_groups, metrics=["average","indicator","normalized"], step="04",force=False):
        final_path = os.path.join(self.output_folder,step)
        mf.create_review_folders(final_path,sm=False)
        final_path = os.path.join(final_path,"OK")
        final_file = os.path.join(final_path,"OK",self.outputs_name + ".csv")
        # Validating if final file exist
        if force or not os.path.exists(final_file):
            groups = df_groups["group"].drop_duplicates()
            for idx,value in groups.items():
                # Create global dataframe
                df = pd.read_csv(os.path.join(self.output_folder,"01","OK",self.outputs_name + ".csv"), encoding = self.encoding)
                # Filtering for group
                crop_list = df_groups.loc[df_groups["group"]==value,"crop"].drop_duplicates()
                df = df.loc[df["crop"].isin(crop_list),:]
                # Updating names
                df = pd.merge(df,new_names,how='left',left_on="crop", right_on="old")
                df.loc[~df["new"].isna(),"crop"] = df.loc[~df["new"].isna(),:]["new"]
                df.drop(["new","old"],axis=1,inplace=True)
                
                # Remove records with NA's
                df = df[df['average'].notna()]

                df = self.create_indicator(df,conf_indicator)
                tmp_path = os.path.join(final_path,value)
                mf.mkdir(tmp_path)
                self.format_df_indicator(df,metrics,tmp_path)
        else:
            print("\tNot processed: " + final_file)
        return final_file
    
    def set_level(self,indicator):
        level = "metric"
        if indicator.startswith("idx-domain"):
            level = "domain"
        elif indicator.startswith("idx-component"):
            level = "component"
        elif indicator.startswith("idx-group"):
            level = "group"
        elif indicator.startswith("idx-cro"):
            level = "crop"
        return level
    
    # Method that joins all 
    # (string) location: path of input file
    # (string[]) metrics: array of metrics that want to export
    # (string) step: prefix of the output files. By default it is 04
    # (bool) force: Set if the process have to for the execution of all files even if the were processed before. 
    #               By default it is False
    def prepare_tableau(self, metrics=["average","indicator","normalized"], step="05",force=False):
        source_path = os.path.join(self.output_folder,"04","OK")
        final_path = os.path.join(self.output_folder,step)
        mf.create_review_folders(final_path,sm=False)
        final_path = os.path.join(final_path,"OK")
        final_file = os.path.join(final_path,self.outputs_name + ".csv")
        # Validating if final file exist
        if force or not os.path.exists(final_file):
            uses_folders = glob.glob(os.path.join(source_path,"*"))
            df_all = pd.DataFrame()
            for use_f in uses_folders:
                print("\tProcessing use:",use_f)
                for metric in metrics:
                    file = os.path.join(use_f,"indicator_" + metric + ".csv")
                    print("\t\tLoading:",file)
                    df = pd.read_csv(file, encoding = self.encoding)
                    value_vars = set(df.columns) - set(['crop'])
                    df = pd.melt(df, id_vars=['crop'], value_vars=value_vars, var_name='indicator', value_name='value')
                    df["metric"] = metric
                    df["level"] = df.apply(lambda x: self.set_level(x["indicator"]), axis=1)
                    df["use"] = use_f.split("/")[-1]
                    df_all = pd.concat([df_all,df], ignore_index=True)
            df_all.to_csv(final_file, index = False, encoding = self.encoding)
        else:
            print("\tNot processed: " + final_file)
        return final_file