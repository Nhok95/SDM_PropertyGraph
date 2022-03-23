from genericpath import isfile
import pandas as pd
import os
from os.path import isfile, join
import re

import constant



def get_listHeaderPair(files):
    r = re.compile('output_[a-zA-Z]*.csv')
    l1 = list(filter(r.match, files))

    r2 = re.compile('output_[a-zA-Z]*_header.csv')
    l2 = list(filter(r2.match, files))
    
    return l1,l2

def cleanDF(df):

    columns = []

    for col in list(df.columns):
        #print(col)
        #col = re.sub('(:[a-z]*)','',col) 
        col = re.sub('(:(?=[a-z]))[a-z\[\]]*', '', col)
        col = re.sub('[a-z]*:', '', col)
        #print(col)
        columns.append(col)
    
    df.columns = columns
    df.dropna(axis='columns', how='all', inplace=True)
    df.dropna(axis='index', how='all', inplace=True)

    print("shape: {}".format(df.shape))

    return df

#########################################

path = join(os.getcwd(),'source')
pathWH = join(path,'withHeader')
pathClean = join(os.getcwd(),'cleaned')

try:
    os.makedirs(pathClean, exist_ok=False)
except FileExistsError as error:
    print("directory '{}' already exists".format(pathClean))


files = [f for f in os.listdir(path) if isfile(join(path, f))]
filesWH = [f for f in os.listdir(pathWH) if isfile(join(pathWH, f))]

list1, list2 = get_listHeaderPair(files)


for file,header in zip(list1,list2):
    print("{}-------{}".format(file,header))
    df_header = pd.read_csv(join(path, header), sep=';', encoding='utf8')
    header = list(df_header.columns)
    print(header)

    df = pd.read_csv(join(path, file), sep=';', encoding='utf8',
                names=header, low_memory=False, nrows=constant.N)

    df_cleaned = cleanDF(df)
    df_cleaned.to_csv(join(pathClean, file), sep=';', encoding='utf8', index=False)

    print(df_cleaned)


for file in filesWH:
    print("#################")
    print(file)
    df = pd.read_csv(join(pathWH, file), sep=';', encoding='utf8', nrows=constant.N)

    df_cleaned = cleanDF(df)
    df_cleaned.to_csv(join(pathClean, file), sep=';', encoding='utf8', index=False)
