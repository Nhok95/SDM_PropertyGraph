import pandas as pd
import os
from os.path import isfile, join
import lorem
import random
import numpy as np

pathClean = join(os.getcwd(),'cleaned')
pathCleanWH = join(pathClean,'withHeader')
pathOut = join(os.getcwd(), 'extracted')


def random_keywords():
	keywords=['Data Structures', 'Machine Learning', 'Big Data', 'Semantic Data', 
	'Relational Databases', 'NLP', 'Data Mining', 'Data Engineering', 'High Performance Computing',
	'Computer Networks']

	samp = random.sample(keywords, random.randint(1,3))

	return '|'.join(samp)


def lorem_abstract_generator():
	return lorem.paragraph()


def journals_and_articles(k):
	df = pd.read_csv(join(pathClean, 'output_article.csv'), sep=';', encoding='utf8')
	df2 = pd.read_csv(join(pathCleanWH, 'org_shuffle.csv'), sep=';', encoding='utf-8')
	# Just for test get only the first 100 rows
	df = df.head(k)

	# key -> article id in the system (not isbn or doi)
	# title -> paper title
	# journal == name
	# year -> to create the relationship and include it as a new instance of Year.
	# volume
	df = df.filter(['key', 'title', 'author', 'ee', 'journal', 'volume', 'year'])
	df['year'] = pd.to_numeric(df['year'], errors='ignore')
	df['abstract'] = [lorem_abstract_generator()]*len(df)
	df['keywords'] = [random_keywords() for i in range(0,len(df))]
	df[['organization', 'type']] = df2.head(k)

	df = df.dropna()
	
	df.to_csv(join(pathOut,'journals_extracted.csv'), sep=';', encoding='utf8', index=False)

def get_authors_pool():
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
		reviewers[index] = "|".join(random.choices(rev,k=3))

	df_articles['reviewers'] = reviewers
	df_articles.to_csv(join(pathOut,'journals_extracted.csv'), sep=';', encoding='utf8', index=False)



#journals_and_articles(100)
#get_authors_pool()
add_random_reviewers_articles()