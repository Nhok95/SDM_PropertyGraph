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

    print("original shape: {}".format(df.shape))

    columns = []

    for col in list(df.columns):
        print(col)
        #col = re.sub('(:[a-z]*)','',col) 
        col = re.sub('(:(?=[a-z]))[a-z\[\]]*', '', col)
        col = re.sub('[a-z]*:', '', col)
        print(col)
        columns.append(col)
    
    df.columns = columns

    if df.shape[0] > constant.N and df.shape[1] > 2:
        df = df.sample(n=constant.N)

    df = df.dropna(axis='index', how='all')

    print("new shape: {}".format(df.shape))

    return df

#########################################

path = join(os.getcwd(),'source')
pathWH = join(path,'withHeader')
pathClean = join(os.getcwd(),'cleaned')
pathCleanWH = join(pathClean,'withHeader')

try:
    os.makedirs(pathClean, exist_ok=False)
except FileExistsError as error:
    print("directory '{}' already exists".format(pathClean))

try:
    os.makedirs(pathCleanWH, exist_ok=False)
except FileExistsError as error:
    print("directory '{}' already exists".format(pathCleanWH))

print(path)

files = [f for f in os.listdir(path) if isfile(join(path, f))]
filesWH = [f for f in os.listdir(pathWH) if isfile(join(pathWH, f))]

list1, list2 = get_listHeaderPair(files)

'''
df_header = pd.read_csv(join(path,'output_article_header.csv'), sep=';', encoding='utf8')
header = list(df_header.columns)
print(header)

df = pd.read_csv(join(path, 'output_article.csv'), sep=';', encoding='utf8',header=None, names=header, low_memory=False, nrows=100) # nrows=2000000   2805754
df.columns = rename_columns(df)

if df.shape[0] >= 2000000:
    df = df.sample(n=2000000)

df_cleaned = df.dropna(axis='columns', how='all')
print(df_cleaned)


'''
for file,header in zip(list1,list2):
    print("{}-------{}".format(file,header))
    df_header = pd.read_csv(join(path, header), sep=';', encoding='utf8')
    header = list(df_header.columns)
    print(header)

    df = pd.read_csv(join(path, file), sep=';', encoding='utf8',header=None, names=header, low_memory=False)
    
    # conferences
    

    df_cleaned = cleanDF(df)
    df_cleaned.to_csv(join(pathClean, file), sep=';', encoding='utf8', index=False)

    print(df_cleaned)


for file in filesWH:
    print("#################")
    print(file)
    df = pd.read_csv(join(pathWH, file), sep=';', encoding='utf8')

    df_cleaned = cleanDF(df)
    df_cleaned.to_csv(join(pathCleanWH, file), sep=';', encoding='utf8', index=False)
