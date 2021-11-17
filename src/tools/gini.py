import os
import pandas as pd
import numpy as np


# Method which calculates interdependence of crops
# (dataframe) data: Dataframe with crop information
# (int[]) years: List of year that should be calculated
# (int) fill: Indicates how many zeros it should add to data to fix all records
def gini_crop(data,years,fill=0):
    
    # Create an output
    print("\t\tCalculating gini")
    df = data.groupby(["Element","crop"])[years].agg(gini_inverse, fill=fill)
    #print(df.index)
    df.reset_index(level=["Element","crop"], inplace=True)
    return df

# Method which calculates 1-gini indicator
# (array) X: values to be processed
# (int) fill: Indicates how many zeros it should add to data to fix all records
def gini_inverse(X,fill=0):
    # (Warning: This is a concise implementation, but it is O(n**2)
    # in time and memory, where n = len(x).  *Don't* pass in huge
    # samples!)
    x = np.sort(X)
    if fill > 0:
        x = np.append(x,np.zeros(fill-len(x)))
    # Mean absolute difference
    mad = np.abs(np.subtract.outer(x, x)).mean()
    # Relative mean absolute difference
    rmad = mad/np.mean(x)
    # Gini coefficient
    g = 0.5 * rmad
    return 1.0-g
