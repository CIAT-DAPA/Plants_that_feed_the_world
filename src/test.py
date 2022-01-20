import os
import re
import glob

from pandas.core import groupby
from requests.models import Response
import numpy as np
import pandas as pd
import tools.manage_files as mf
import requests


file = "/indicator/data/inputs/02_raw/wikipedia/01/OK/2019-wikipedia.csv"
df = pd.read_csv(file)
stack = pd.melt(df, id_vars=['crop'], value_vars=['genus_count','common_name_count','taxa_count'])
print(stack.head())