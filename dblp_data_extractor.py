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
	#We get the 30 countries with higher HD Index.
	df_hdi = pd.read_csv(join(pathData, 'hdi-countries.csv'), sep=',', encoding='utf8', nrows=30)
	
	df_city = df_city.filter(['name','country'])
	country_list = list(df_hdi['country'])

	# We get 3 random cities from each country (replace is True due to there are some countries with less than 3 
	# and, in these cases, we want to get those cities even if we have to double them)
	df_sample = df_city.groupby('country').sample(n=3, replace=True, random_state=1)

	# We remove duplicates created in the previous operation
	df_sample.drop_duplicates(inplace=True) 

	# We return only high HDI countries (just to filter to a small subset)
	df = df_sample[df_sample.country.isin(country_list)]
	return  df.reset_index(drop=True)

def lorem_generator():
	return lorem.paragraph()


def journals_and_articles(k):
	df = pd.read_csv(join(pathClean, 'output_article.csv'), sep=';', encoding='utf8', low_memory=False)
	df2 = pd.read_csv(join(os.getcwd(), 'org_shuffle.csv'), sep=';', encoding='utf-8', low_memory=False)
	# Just for test get only the first 100 rows
	df = df.head(k)

	# key -> article id in the system (not isbn or doi)
	# title -> paper title
	# journal == name
	# year -> to create the relationship and include it as a new instance of Year.
	# volume
	df = df.filter(['key', 'title', 'author', 'ee', 'journal', 'volume', 'year'])
	df['year'] = pd.to_numeric(df['year'], errors='ignore')
	df['abstract'] = [lorem_generator()]*len(df)
	df['keywords'] = [random_keywords() for i in range(0,len(df))]
	df[['organization', 'type']] = df2.head(k)

	df = df.dropna()
	
	df.to_csv(join(pathOut,'journals_extracted.csv'), sep=';', encoding='utf8', index=False)

def get_reviewers_random_pool():
	df = pd.read_csv(join(pathOut,'journals_extracted.csv'), sep=';', encoding='utf-8')

	authors = df['author'].str.split('|').apply(pd.Series).stack().reset_index(drop=True)
	authors = authors.drop_duplicates()
	df_authors = authors.to_frame()
	df_authors['reviewer'] = df_authors
	df_authors = df_authors['reviewer']

	df_authors.to_csv(join(pathOut, 'reviewers.csv'), sep=';', encoding='utf-8', index=False)

def add_random_reviewers_articles():
	df_articles = pd.read_csv(join(pathOut,'journals_extracted.csv'), sep=';', encoding='utf-8')
	df_reviewers = pd.read_csv(join(pathOut,'reviewers.csv'), sep=';', encoding='utf-8')

	rev_pool = df_reviewers['reviewer'].tolist()

	reviewers = [None]*len(df_articles)

	for index, row in df_articles.iterrows():
		authors = set(row['author'].split('|'))
		rev = [r for r in rev_pool if r not in authors]
		random.shuffle(rev)
		reviewers[index] = "|".join(rev[:3])

	df_articles['reviewers'] = reviewers
	df_articles['review'] = [lorem_generator()]*len(df_articles)
	df_articles['decision'] = ['accepted']*len(df_articles)
	df_articles.to_csv(join(pathOut,'journals_extracted.csv'), sep=';', encoding='utf8', index=False)

def conferences():
	df_cities = getCities() #83 cities
	print(df_cities.shape)

	df = pd.read_csv(join(pathClean, 'output_inproceedings.csv'), sep=';', encoding='utf8', low_memory=False)
	df2 = pd.read_csv(join(pathClean, 'output_proceedings.csv'), sep=';', encoding='utf8', low_memory=False)
	
	# key -> article id in the system (not isbn or doi)
	# title -> paper title
	# author -> array of authors
	# booktitle -> conference name
	# year -> to create the relationship and include it as a new instance of Year.
	df = df.filter(['key','title','author','booktitle','year'])
	df['year'] = pd.to_numeric(df['year'], errors='ignore')
	df['abstract'] = [lorem_generator()]*len(df)
	df['keywords'] = [random_keywords() for i in range(0,len(df))]
	print(df.shape)

	# key -> article id in the system (not isbn or doi)
	# title -> paper title
	# booktitle -> conference name
	# year -> to create the relationship and include it as a new instance of Year.
	df2 = df2.filter(['key','title','booktitle','year'])
	df2['year'] = pd.to_numeric(df2['year'], errors='ignore')

	df = df.dropna(axis='index')
	df2 = df2.dropna(axis='index')

	# Edition and cities
	rng = np.random.default_rng()
	random_indexes = rng.integers(0, len(df_cities), size=len(df2))

	cities = [[],[]]
	for i in random_indexes:
		cities[0].append(df_cities['name'].iloc[i])
		cities[1].append(df_cities['country'].iloc[i])

	df2['city'] = cities[0]
	df2['country'] = cities[1]
	# We assume a edition is identified by a the conference name, a city and a year
	#insert edition
	df2['edition'] = [str(x) for x in df2['year']]
	df2['edition'] = df2['booktitle'] + "_" + df2['city'] + "_"+ df2['edition']

	# Just for test get only the first 100 rows
	df = df.head(constant.N_TEST)
	df2 = df2.head(constant.N_TEST)

	df.to_csv(join(pathOut,'conferences_extracted.csv'), sep=';', encoding='utf8', index=False)
	df2.to_csv(join(pathOut,'conferences_extracted2.csv'), sep=';', encoding='utf8', index=False)



journals_and_articles(constants.N_TEST)
get_reviewers_random_pool()
add_random_reviewers_articles()

conferences()
