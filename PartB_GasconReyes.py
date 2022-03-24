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
        with self.driver.session() as session:
            result = session.run("""
            	MATCH (c:Conference)-->(p:Paper)<--(p2:Paper)
				WITH c.title AS Conf, p as Paper, count(p2) AS Citations
				ORDER BY Citations DESC
				WITH Conf, collect(Paper.title) AS top
				RETURN Conf, top[..3] 
				ORDER BY Conf ASC
            """).data()

        for i in result:
            print(i.get('Conf'), ':', i.get('top[..3]'), '\n')

    
    def query2(self):
        with self.driver.session() as session:
            result = session.run("""
            	MATCH (c:Conference)-->(p:Paper)-[:AUTHOR]->(r:Researcher) 
				WITH c, r, count(c) AS Editions
				WHERE Editions >= 4
				RETURN c.title, collect(r.name) AS Community

   			""").data()

        for i in result:
            print(i.get('c.title'), ':', i.get('Community'), '\n')

    def query3(self):
        with self.driver.session() as session:
            result = session.run("""
            	MATCH (j:Journal)-->(p:Paper)<-[:CITES]-(p2:Paper)
				WITH j.title as title, j.year as year, p, p2
				WHERE toInteger(year) IN [date().year-1,date().year-2]
				WITH title, year, count(p) as Publications, count(p2) as Citations
				RETURN title, sum(Citations)/sum(Publications) as ImpactFactor
            """).data()
        
        for i in result:
            print(i.get('title'), ':', i.get('ImpactFactor'), '\n')


    def query4(self):
        with self.driver.session() as session:
            result = session.run("""
            	MATCH (r:Researcher)<-[:AUTHOR]-(p:Paper)<--(c:Paper)
				WITH r.name AS Author, p, count(c.title) AS PCitations
				ORDER BY Author, PCitations DESC
				WITH Author, collect(PCitations) AS Citations
				UNWIND range(0, size(Citations)-1) AS it
				WITH Author,
				CASE WHEN Citations[it] <= (it+1)
 					THEN Citations[it]
 					ELSE (it+1)
				END AS HI
				RETURN Author, max(HI) AS HIndex
				ORDER BY HIndex, Author DESC
            """).data()
        
        for i in result:
        	print(i.get('Author'), ':', i.get('HIndex'), '\n')


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
        print('Executing query 3: Calculating the impact factor of this year...')
        result = query.query3()
    
    elif args.query == '4':
        print('Executing query 4: Calculating the HIndex...')
        result = query.query4()

    query.close()