from asyncio import constants
import pandas as pd
import os
from os.path import join
import lorem
import random
import numpy as np

import constant

pathClean = join(os.getcwd(),'cleaned')
pathData = join(os.getcwd(),'data')
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

def getCities():
	df_city = pd.read_csv(join(pathData, 'world-cities.csv'), sep=',', encoding='utf8')
	#We get the 50 countries with higher HD Index.
	df_hdi = pd.read_csv(join(pathData, 'hdi-countries.csv'), sep=',', encoding='utf8', nrows=50)
	
	
	df_city = df_city.filter(['name','country'])
	country_list = list(df_hdi['country'])

	# We get 3 random cities of every country (replace is True due to there are some countries with less than 3 
	# and, in these cases, we want to get that cities even if we have to duplicate it)
	df_sample = df_city.groupby('country').sample(n=3, replace=True, random_state=1)

	# We remove duplicates created in the previous operation
	df_sample.drop_duplicates(inplace=True) 

	#We return only countries with high HDI (just to filter to a small subset)
	return df_sample[df_sample.country.isin(country_list)] 

def lorem_abstract_generator():
	return lorem.paragraph()

def conferences():
	df_cities = getCities()

	df = pd.read_csv(join(pathClean, 'output_inproceedings.csv'), sep=';', encoding='utf8', low_memory=False)
	df2 = pd.read_csv(join(pathClean, 'output_proceedings.csv'), sep=';', encoding='utf8', low_memory=False)
	
	# booktitle -> conference name
	# author -> author array
	# year -> to create the relationship and include it as a new instance of Year.
	df = df.filter(['booktitle','author','key','year'])
	df['year'] = pd.to_numeric(df['year'], errors='ignore')
	df['abstract'] = [lorem_abstract_generator()]*len(df)
	df['keywords'] = [random_keywords() for i in range(0,len(df))]
	print(df.shape)

	# We assume a edition is identified by a city 
	df2 = df2.filter(['booktitle', 'title', 'key', 'year'])

	df2['year'] = pd.to_numeric(df2['year'], errors='ignore')
	#df2[['city','country']] = df_cities.sample(n=len(df))
	

	

	print("HEAD:")
	print(df.head())
	#print(df2.head())

	df = df.dropna(axis='index')
	df2 = df2.dropna(axis='index')
	print("NEW SHAPE:")
	print(df.shape)
	#print(df2.shape)

	# Just for test get only the first 100 rows
	df = df.head(constant.N_TEST)
	df2 = df2.head(constant.N_TEST)

	df.to_csv(join(pathOut,'conferences_extracted.csv'), sep=';', encoding='utf8', index=False)
	#df2.to_csv(join(pathOut,'conferences_extracted.csv'), sep=';', encoding='utf8', index=False)


conferences()
#getCities()