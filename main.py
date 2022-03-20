# !pip install wheel
# !pip install neo4j
import argparse
from loader import Neo4JLoader
from loader2 import Neo4JLoader2

def GetParserArgs():
    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--mode", default='load',
                        help="Select mode", 
                        type=str, choices=['load','clean'])
    parser.add_argument("-u", "--user", default='neo4j',
                        help="username", 
                        type=str)
    parser.add_argument("-p", "--password",
                        help="password", 
                        type=str)

    return parser.parse_args()



if __name__ == "__main__":
    args = GetParserArgs()

    if args.password == None:
        loader = Neo4JLoader("bolt://localhost:7687")
    else:
        loader = Neo4JLoader("bolt://localhost:7687", (args.user, args.password))

    if args.mode == 'load':
        print('Start bulk Loading...')
        #loader.load_authors()
        loader.load_journals_articles()
        loader.load_authors_articles()
        loader.load_paper_citations()

    elif args.mode == 'clean':
        print('Cleaning graph...')
        loader.clean_all()

    loader.close()


