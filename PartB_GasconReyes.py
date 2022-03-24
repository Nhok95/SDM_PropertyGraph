import argparse
from neo4j import GraphDatabase


def getParserArgs():
    parser = argparse.ArgumentParser()
    parser.add_argument("-q", "--query", default='1',
                        help="Select query", 
                        type=str, 
                        choices=['1','2','3','4'])
    parser.add_argument("-u", "--user", default='neo4j',
                        help="username", 
                        type=str)
    parser.add_argument("-p", "--password",
                        help="password", 
                        type=str)

    return parser.parse_args()

class Neo4JQueries:

    def __init__(self, uri, auth_value=None):

        print("auth: {}".format(auth_value))
        self.driver = GraphDatabase.driver(uri, auth=auth_value)

    def close(self):

        print("##########################")
        print("Connection closed")
        self.driver.close()
    
    def query1(self):
        print("Inserting journals and articles...")
        with self.driver.session() as session:
            result = session.run("""
                """)

            if self.printResult(result):
                print("Journals and articles inserted")
            else:
                print("Error inserting journals and articles")

    def query2(self):
        print("Inserting journals and articles...")
        with self.driver.session() as session:
            result = session.run("""
                """)

            if self.printResult(result):
                print("Journals and articles inserted")
            else:
                print("Error inserting journals and articles")

    def query3(self):
        print("Inserting journals and articles...")
        with self.driver.session() as session:
            result = session.run("""
                """)

            if self.printResult(result):
                print("Journals and articles inserted")
            else:
                print("Error inserting journals and articles")

    def query4(self):
        print("Inserting journals and articles...")
        with self.driver.session() as session:
            result = session.run("""
                """)

            if self.printResult(result):
                print("Journals and articles inserted")
            else:
                print("Error inserting journals and articles")


if __name__ == "__main__":
    args = getParserArgs()

    if args.password == None:
        query = Neo4JQueries("bolt://localhost:7687")
    else:
        query = Neo4JQueries("bolt://localhost:7687", (args.user, args.password))

    if args.query == '1':
        print('Executing query 1: Finding top 3 most cited papers...')
        result = query.query1()
    
    elif args.query == '2':
        print('Executing query 2: Finding community...')
        result = query.query2()
    
    elif args.query == '3':
        print('Executing query 3: Find top 3 most cited papers...')
        result = query.query3()
    
    elif args.query == '4':
        print('Executing query 4: Find top 3 most cited papers...')
        result = query.query4()

    print(result)

    query.close()

