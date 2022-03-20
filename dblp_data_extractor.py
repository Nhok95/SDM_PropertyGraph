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

def journals_and_articles():
	df = pd.read_csv(join(pathClean, 'output_article.csv'), sep=';', encoding='utf8', low_memory=False)

	# key -> article id in the system (not isbn or doi)
	# title -> paper title
	# journal == name
	# year -> to create the relationship and include it as a new instance of Year.
	# volume
	df = df.filter(['key', 'title', 'author', 'ee', 'journal', 'volume', 'year'])
	df['year'] = pd.to_numeric(df['year'], errors='ignore')
	df['abstract'] = [lorem_abstract_generator()]*len(df)
	df['keywords'] = [random_keywords() for i in range(0,len(df))]

	df = df.dropna()

	# Just for test get only the first 100 rows
	df = df.head(constants.N_TEST)
	
	#print(df.head(1)['author'])
	#print(type(df.head(1)['author']))
	df.to_csv(join(pathOut,'journals_extracted.csv'), sep=';', encoding='utf8', index=False)



journals_and_articles()