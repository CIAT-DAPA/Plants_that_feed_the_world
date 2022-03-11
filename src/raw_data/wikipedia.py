import os
import re
import glob

from pandas.core import groupby
from requests.models import Response
import numpy as np
import pandas as pd
import tools.manage_files as mf
import requests



class Wikipedia(object):
    
    url = ''
    timing = ''    
    encoding = ''
    years = []

    ## Method construct
    def __init__(self, url='https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/en.wikipedia/all-access/all-agents/',
                timing = 'monthly', years = [2015,2016,2017,2018], encoding="ISO-8859-1"):
        self.url = url
        self.timing = timing
        self.encoding = encoding
        self.years = years

    # Method that search how many views had a term in wikipedia
    # it uses the wikipedia media api.
    # it prints when url doesn't have views.
    # (string) url: url to search data. the base url is saved into url property
    # return: Total number of views of the url
    def request_wikipedia(self, url):                
        views = 0
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
        resp = requests.get(url,headers=headers)
        if resp.status_code == 200:
            data = resp.json()                                    
            views = sum([int(x["views"]) for x in data["items"] ])
        if views == 0:
            print("\t\t\t", url, "Not found")
        return views


    # Method that counts how many countries have crops and terms related to them.
    # It generates two types of files: in OK you can see by each variable per year and
    # the SM you can extract the final file
    # (string) path: Location where the files should be saved    
    # (dataframe) crops_genus: Sheet name where data is saved
    # (dataframe) taxa: Field name which has the crop name
    # (dataframe) common_names: Field name which has the crop name
    # (string) step: prefix of the output files. By default it is 01
    # (bool) force: Set if the process have to for the execution of all files even if the were processed before. 
    #               By default it is False
    def get_pageviews(self, path, crops_genus, taxa, common_names, step="01",force = False):
        final_path = os.path.join(path,"wikipedia",step)
        mf.create_review_folders(final_path, er=False)

        final_file = os.path.join(final_path,"SM","wikipedia.csv")
        # It checks if files should be force to process again or if the path exist
        if force or not os.path.exists(final_file):
            df_final = pd.DataFrame()
            df = crops_genus
            df_taxa = pd.merge(crops_genus,taxa,on="crop",how="inner")
            df_common = common_names
            # Searching by years
            for y in self.years:
                print("\tYear:", y)
                print("\t\tType: common name")
                df_common["common_name_count"] = df_common["common_name"].apply(lambda x: self.request_wikipedia(self.url + x + "/" + self.timing + "/" + str(y) + "010100/" + str(y) + "123100"))
                print("\t\tType: genus")
                df["genus_count"] = crops_genus["genera"].apply(lambda x: self.request_wikipedia(self.url + x + "/" + self.timing + "/" + str(y) + "010100/" + str(y) + "123100"))
                print("\t\tType: taxa")
                df_taxa["taxa_count"] = df_taxa["taxa"].apply(lambda x: self.request_wikipedia(self.url + x + "/" + self.timing + "/" + str(y) + "010100/" + str(y) + "123100"))
                print("\t\tGrouping common and taxa")
                df_common = df_common.groupby(["crop"],as_index=False)["common_name_count"].sum()                
                df_taxa = df_taxa.groupby(["crop"],as_index=False)["taxa_count"].sum()
                df = pd.merge(df,df_common,on="crop",how="left")
                df = pd.merge(df,df_taxa,on="crop",how="left")
                df.to_csv(os.path.join(final_path,"OK",str(y) + "-wikipedia.csv"), index = False, encoding = self.encoding)
                
                df_temp = pd.melt(df, id_vars=['crop'], value_vars=['genus_count','common_name_count','taxa_count'])
                df_temp.columns = ["crop","Element","Y" + str(y)]
                if df_final.shape[0] == 0:
                    df_final = df_temp
                else:
                    df_final = pd.merge(df_final,df_temp,how="cross",on=["crop","Element"])
            
            df_final.to_csv(final_file, index = False, encoding = self.encoding)


        else:
            print("\tNot processed: " + final_file)    
