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

    def request_wikipedia(self, url):                
        views = 0
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
        resp = requests.get(url,headers=headers)
        if resp.status_code == 200:
            data = resp.json()                                    
            views = sum([int(x["views"]) for x in data["items"] ])
        return views


    # Method that counts how many countries have crops.
    # (string) path: Location where the files should be saved    
    # (string) crops_genus: Sheet name where data is saved
    # (string) taxa: Field name which has the crop name
    # (string) step: prefix of the output files. By default it is 01
    # (bool) force: Set if the process have to for the execution of all files even if the were processed before. 
    #               By default it is False
    def get_pageviews(self, path, crops_genus, taxa, step="01",force = False):
        final_path = os.path.join(path,"wikipedia",step)
        mf.create_review_folders(final_path)

        final_file = os.path.join(final_path,"OK","wikipedia.csv")
        # It checks if files should be force to process again or if the path exist
        if force or not os.path.exists(final_file):
            df = crops_genus
            df_taxa = pd.merge(crops_genus,taxa,on="crop",how="inner")
            # Searching by years
            for y in self.years:
                print("\tYear:", y)
                print("\t\tType: common name")
                df["common_name_count"] = crops_genus["crop"].apply(lambda x: self.request_wikipedia(self.url + x + "/" + self.timing + "/" + str(y) + "010100/" + str(y) + "123100"))
                print("\t\tType: genus")
                df["genus_count"] = crops_genus["genera"].apply(lambda x: self.request_wikipedia(self.url + x + "/" + self.timing + "/" + str(y) + "010100/" + str(y) + "123100"))
                print("\t\tType: taxa")
                df_taxa["taxa_count"] = df_taxa["taxa"].apply(lambda x: self.request_wikipedia(self.url + x + "/" + self.timing + "/" + str(y) + "010100/" + str(y) + "123100"))
                print("\t\tGrouping taxa")
                df_taxa = df_taxa.groupby(["crop"],as_index=False)["taxa_count"].sum()                
                df = pd.merge(df,df_taxa,on="crop",how="inner")
                
            df.to_csv(os.path.join(final_path,"OK","wikipedia.csv"), index = False, encoding = self.encoding)            
        else:
            print("\tNot processed: " + final_file)    
