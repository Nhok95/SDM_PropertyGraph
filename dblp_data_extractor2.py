from asyncio import constants
import pandas as pd
import os
from os.path import join
import lorem
import random
import numpy as np

import constant

pathClean = join(os.getcwd(),'cleaned')
pathCleanWH = join(pathClean,'withHeader')
pathOut = join(os.getcwd(), 'extracted')

try:
    os.makedirs(pathOut, exist_ok=False)
except FileExistsError as error:
    print("directory '{}' already exists".format(pathOut))

def random_keywords():
	keywords=['Data Structures', 'Machine Learning', 'Big Data', 'Semantic Data', 
	'Relational Databases', 'NLP', 'Data Mining', 'Data Engineering', 'High Performance Computing',
	'Computer Networks']

	samp = random.sample(keywords, random.randint(1,3))

	return '|'.join(samp)


def lorem_abstract_generator():
	return lorem.paragraph()

def conferences():
	df = pd.read_csv(join(pathClean, 'output_proceedings.csv'), sep=';', encoding='utf8', low_memory=False)
	df2 = pd.read_csv(join(pathClean, 'output_inproceedings.csv'), sep=';', encoding='utf8', low_memory=False)

	#df = df.filter([])

	df2 = df2.filter(['booktitle','author','key','year'])
	df2['year'] = pd.to_numeric(df2['year'], errors='ignore')
	df2['abstract'] = [lorem_abstract_generator()]*len(df2)
	df2['keywords'] = [random_keywords() for i in range(0,len(df2))]
	print(df2.shape)

	print("HEAD:")
	print(df.head())
	print(df2.head())

	df = df.dropna()
	df2 = df2.dropna()
	print("NEW SHAPE:")
	print(df.shape)
	print(df2.shape)

	# Just for test get only the first 100 rows
	df = df.head(constant.N_TEST)
	df2 = df2.head(constant.N_TEST)

	df2.to_csv(join(pathOut,'conferences_extracted.csv'), sep=';', encoding='utf8', index=False)


conferences()