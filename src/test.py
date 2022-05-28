import os
import re
import glob

from pandas.core import groupby
from requests.models import Response
import numpy as np
import pandas as pd
import tools.manage_files as mf
import requests


#file = "/indicator/data/inputs/02_raw/wikipedia/01/OK/2019-wikipedia.csv"
#df = pd.read_csv(file)
#stack = pd.melt(df, id_vars=['crop'], value_vars=['genus_count','common_name_count','taxa_count'])
#print(stack.head())

"""
import entrezpy.conduit
import requests
import xmltodict
from xml.etree import ElementTree
c = entrezpy.conduit.Conduit('h.sotelo@cgiar.org')
fetch_taxonomy = c.new_pipeline()
print("Searching")
sid = fetch_taxonomy.add_search({'db' : 'taxonomy', 'term' : 'Abelmoschus esculentus'})
print("Found sid", sid)
#fid = fetch_influenza.add_fetch({'retmax' : 10, 'retmode' : 'text', 'rettype': 'fasta'}, dependency=sid)
fid = fetch_taxonomy.add_fetch({}, dependency=sid)
xml = c.run(fetch_taxonomy)
print("xml",xml.get_result())
dsdocs = xmltodict.parse(xml)
print(dsdocs["TaxaSet"]["Taxon"])

""
import entrezpy.conduit
email = 'h.sotelo@cgiar.org'
c = entrezpy.conduit.Conduit(email)
fetch_taxonomy = c.new_pipeline()
sid = fetch_taxonomy.add_search({'db' : 'taxonomy', 'term' : 'Abelmoschus esculentus'})
print("Found sid", sid)

#import entrezpy.esummary.esummarizer
import entrezpy.elink.elinker

#e = entrezpy.esummary.esummarizer.Esummarizer(sid,
e = entrezpy.elink.elinker.Elinker(sid,
                                              email,
                                              apikey=None,
                                              apikey_var=None,
                                              threads=None,
                                              qid=None)

analyzer = e.inquire({'db' : 'taxonomy', 'id' : [455045]})
#print(analyzer.get_result().summaries)
print(analyzer.count, analyzer.retmax, analyzer.retstart, analyzer.uids)
"""
"""
import entrezpy.esearch.esearcher

e = entrezpy.esearch.esearcher.Esearcher('esearcher', 'h.sotelo@cgiar.org')
a = e.inquire({'db':'taxonomy','term':'Abelmoschus esculentus'} )
print(a.size())
print(a.result.dump())
print(a.result.references)
#print(a.reference().webenv, a.reference().)
"""

# Get the uid for taxon
#import entrezpy.esearch.esearcher

"""
email = 'h.sotelo@cgiar.org'
taxon = 'Abelmoschus esculentus'
e = entrezpy.esearch.esearcher.Esearcher('esearcher', email)
print("searching ids")
a = e.inquire({'db':'taxonomy','term':taxon, 'retmax': 110000, 'rettype': 'uilist'} )
uids = a.get_result().uids
print("ids",uids)

# Search data
print("searching database")
import entrezpy.conduit
import entrezpy.elink.elinker

c = entrezpy.conduit.Conduit(email)
fetch_taxonomy = c.new_pipeline()
sid = fetch_taxonomy.add_search({'db' : 'taxonomy', 'term' : taxon})
print("sid",sid)
e2 = entrezpy.elink.elinker.Elinker(sid,
                                              email,
                                              apikey=None,
                                              apikey_var=None,
                                              threads=None,
                                              qid=None)
analyzer = e2.inquire({'dbfrom' : 'protein', 'id' : uids})
print(analyzer.get_result().summaries)
print(analyzer.count, analyzer.retmax, analyzer.retstart, analyzer.uids)
"""

import requests
import xmltodict

taxon = 'Abelmoschus esculentus'
databases = ["pmc","nuccore","protein","genome","gene"]

taxon_formated = taxon.replace(" ","+")
url = "https://eutils.ncbi.nlm.nih.gov/gquery?term=" + taxon_formated + "&retmode=xml"
response = requests.get(url)
#print(response.content)
xml = xmltodict.parse(response.content)

for item in xml["Result"]["eGQueryResult"]["ResultItem"]:
    if item["DbName"] in databases:
        print(item["DbName"],":",item["Count"])
