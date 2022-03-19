import pandas as pd
import os
from os.path import isfile, join
import lorem

pathClean = join(os.getcwd(),'cleaned')
pathCleanWH = join(pathClean,'withHeader')
pathOut = join(os.getcwd(), 'extracted')


def lorem_abstract_generator():
	return lorem.paragraph()

def journals_and_articles():
	df = pd.read_csv(join(pathClean, 'output_article.csv'), sep=';', encoding='utf8')

	# ID -> article id in the system (not isbn or doi)
	# title -> paper title
	# key == ISSN ?
	# journal == name
	# year -> to create the relationship and include it as a new instance of Year.
	# volume
	df = df.filter(['ID', 'title', 'author', 'key', 'journal', 'volume', 'year'])
	df['year'] = pd.to_numeric(df['year'], errors='ignore')
	df['abstract'] = [lorem_abstract_generator()]*len(df)

	# Just for test get only the first 100 rows
	df = df.head(100)

	df.to_csv(join(pathOut,'journals_extracted.csv'), sep=';', encoding='utf8', index=False)



journals_and_articles()