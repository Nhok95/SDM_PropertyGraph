import argparse
from neo4j import GraphDatabase


def getParserArgs():
    parser = argparse.ArgumentParser()
    parser.add_argument("-a", "--algorithm", default='1',
                        help="Select query", 
                        type=str, 
                        choices=['Louvain', 'NodeSimilarity'])
    parser.add_argument("-u", "--user", default='neo4j',
                        help="username", 
                        type=str)
    parser.add_argument("-p", "--password",
                        help="password", 
                        type=str)

    return parser.parse_args()

class Neo4JAlgorithms:

    def __init__(self, uri, auth_value=None):

        print("auth: {}".format(auth_value))
        self.driver = GraphDatabase.driver(uri, auth=auth_value)

    def close(self):

        print("##########################")
        print("Connection closed")
        self.driver.close()
    
    def louvain(self):
        print('Executing algorithm 1: Louvain')
        with self.driver.session() as session_a:
            exists = session_a.run("""
                CALL gds.graph.exists('myGraph') YIELD exists RETURN exists
                """).value('exists')[0]

            if not exists:
                print("Creating a new graph in the catalog")
                result = session_a.run("""
                        CALL gds.graph.create(
                            'myGraph',
                            'Paper',
                            {
                                CITES: {
                                    orientation: 'NATURAL'
                                }
                            }
                        )
                        """).value('graphName')[0]
                
                print("{0} graph created".format(result))

        with self.driver.session() as session_b:
                result = session_b.run("""
                        CALL gds.louvain.stream('myGraph')
                        YIELD nodeId, communityId
                        RETURN gds.util.asNode(nodeId).title AS title, communityId
                        ORDER BY title ASC
                        """).values()
                
                for x in result:
                    print(x)
            

    def nodeSimilarity(self, top=True):
        print('Executing algorithm 2: Node Similarity')
        with self.driver.session() as session_a:
            exists = session_a.run("""
                CALL gds.graph.exists('paper-citations') YIELD exists RETURN exists
                """).value('exists')[0]

            if not exists:
                print("Creating a new graph in the catalog")
                result = session_a.run("""
                        CALL gds.graph.create('paper-keyword',
                            ['Paper', 'Keyword'], {
                                TOPIC: {
                                    type: 'TOPIC'
                                }
                            } 
                        );
                        """).value('graphName')[0]
                
                print("{0} graph created".format(result))

        with self.driver.session() as session_b:
            if top:
                result = session_b.run("""
                        CALL gds.nodeSimilarity.stream('paper-keyword') 
                        YIELD node1, node2, similarity
                        RETURN 
                            gds.util.asNode(node1).title AS Paper1, 
                            gds.util.asNode(node2).title AS Paper2, 
                            similarity
                        ORDER BY similarity DESC, Paper1, Paper2 LIMIT 10
                        """).values()
                
                for x in result:
                    print(x)


if __name__ == "__main__":
    args = getParserArgs()

    if args.password == None:
        alg = Neo4JAlgorithms("bolt://localhost:7687")
    else:
        alg = Neo4JAlgorithms("bolt://localhost:7687", (args.user, args.password))

    if args.algorithm == 'Louvain':
        result = alg.louvain()
    
    elif args.algorithm == 'NodeSimilarity':
        result = alg.nodeSimilarity()

    print(result)

    alg.close()

