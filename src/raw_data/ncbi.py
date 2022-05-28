import os
import re
import glob

import numpy as np
import pandas as pd
import tools.manage_files as mf
import requests
import xmltodict



class NCBI(object):

    encoding = ''
    final_file = ''
    url = ''
    retmode = ''

    ## Method construct
    def __init__(self, url="https://eutils.ncbi.nlm.nih.gov/gquery", retmode="xml", final_file = "ncbi.csv", encoding="ISO-8859-1"):
        self.url = url
        self.retmode = retmode
        self.final_file = final_file
        self.encoding = encoding

    # Method that search how many views had a term in wikipedia
    # it uses the wikipedia media api.
    # it prints when url doesn't have views.
    # (string) taxon: taxon to search into ncbi
    # (string[]) databases: List of databases from ncbi
    # return: Number of records for each database
    def request_ncbi(self, taxon, databases):
        data = {}
        taxon_formated = taxon.replace(" ","+")
        url_tmp = self.url + "?term=" + taxon_formated + "&retmode=" + self.retmode
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
        response = requests.get(url_tmp,headers=headers)
        #print(response.content)
        if response.status_code == 200:
            xml = xmltodict.parse(response.content)

            for item in xml["Result"]["eGQueryResult"]["ResultItem"]:
                if item["DbName"] in databases:
                    data[item["DbName"]]=int(item["Count"])
        else:
            for db in databases:
                data[db] = 0
        data["status"] = float(response.status_code)
        return data
    
    # Method that gets count of records into the ncbi's databases.
    # It generates two types of files: in OK you can see count of records by each taxon and
    # the SM you can extract the final file
    # (string) path: Location where the files should be saved
    # (dataframe) taxa: Field name which has the crop name
    # (string[]) dbs_ncbi: array of databases in ncbi
    # (string) step: prefix of the output files. By default it is 01
    # (bool) force: Set if the process have to for the execution of all files even if the were processed before. 
    #               By default it is False
    def download_data(self, path, taxa, dbs_ncbi, year, step="01",force = False):
        final_path = os.path.join(path,"ncbi",step)
        mf.create_review_folders(final_path)

        final_file = os.path.join(final_path,"SM",self.final_file)
        # It checks if files should be force to process again or if the path exist
        if force or not os.path.exists(final_file):
            df_taxa = taxa

            print("\t\tType: taxa")
            df_tmp = df_taxa["taxa"].apply(lambda x: self.request_ncbi(x,dbs_ncbi))
            df_taxa = pd.concat([df_taxa, df_tmp.apply(pd.Series)], axis=1)

            print("\t\tFiltering")
            df_ok = df_taxa.loc[df_taxa["status"]==200,:]
            df_error = df_taxa.loc[df_taxa["status"]!=200,:]

            df_ok.to_csv(os.path.join(final_path,"OK",self.final_file), index = False, encoding = self.encoding)
            df_error.to_csv(os.path.join(final_path,"ER",self.final_file), index = False, encoding = self.encoding)

            print("\t\tAcummulating")
            year_name = "Y" + str(year)
            df = df_taxa.groupby(["crop"],as_index=False)[dbs_ncbi].sum()

            df_final = pd.melt(df, id_vars=['crop'], value_vars=dbs_ncbi)
            df_final.columns = ["crop","Element",year_name]
            df_final.to_csv(os.path.join(final_path,"SM",self.final_file), index = False, encoding = self.encoding)


        else:
            print("\tNot processed: " + final_file)
