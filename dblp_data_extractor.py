from asyncio import constants
import pandas as pd
import os
from os.path import join
import lorem
import random
import numpy as np

import constant

pathClean = join(os.getcwd(),'cleaned')
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



journals_and_articles(constants.N_TEST)
get_reviewers_random_pool()
add_random_reviewers_articles()
