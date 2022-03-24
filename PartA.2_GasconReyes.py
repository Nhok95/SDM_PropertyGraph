import argparse
from loader import Neo4JLoader


def getParserArgs():
    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--mode", default='load',
                        help="Select mode", 
                        type=str, 
                        choices=['load','clean'])
    parser.add_argument("-u", "--user", default='neo4j',
                        help="username", 
                        type=str)
    parser.add_argument("-p", "--password",
                        help="password", 
                        type=str)

    return parser.parse_args()



if __name__ == "__main__":
    args = getParserArgs()

    if args.password == None:
        loader = Neo4JLoader("bolt://localhost:7687")
    else:
        loader = Neo4JLoader("bolt://localhost:7687", (args.user, args.password))

    if args.mode == 'load':
        print('Start bulk Loading...')

        ## Journals ##
        loader.load_journals_articles()
        loader.load_journals_authors_articles()

        ## Conferences ##
        loader.load_conference_articles()
        loader.load_conference_authors_articles()
        loader.load_conference_cities()

        ## Reviews ##
        loader.load_article_reviews()

        ## Cites ##
        loader.load_paper_citations()

    elif args.mode == 'clean':
        print('Cleaning graph...')
        loader.clean_all()

    loader.close()


