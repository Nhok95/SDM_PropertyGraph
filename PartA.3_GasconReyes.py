import argparse
from loader import Neo4JLoader


def getParserArgs():
    parser = argparse.ArgumentParser()
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

    print('Start updating...')

     ## Organizations ##
    loader.load_organizations()

    ## Reviews ##
    loader.update_article_reviews()

    loader.close()


