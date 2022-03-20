from neo4j import GraphDatabase

class Neo4JLoader:

    def __init__(self, uri, auth_value=None):

        print("auth: {}".format(auth_value))
        self.driver = GraphDatabase.driver(uri, auth=auth_value)

    def close(self):

        print("connection closed")
        self.driver.close()
    
    @staticmethod
    def printResult(result, mode="insert"):

        counter = result.consume().counters
        if counter.contains_updates:
            if (mode == "insert"):
                print(
                    '''Added {0} labels, 
                    Created {1} nodes, 
                    Set {2} properties.'''.format(
                        counter.labels_added, 
                        counter.nodes_created, 
                        counter.properties_set)
                    )
            elif (mode == "clean"):
                print(
                    '''Deleted {0} nodes. '''.format(counter.nodes_deleted)
                    )
            elif (mode == "test"):
                print(counter)
        else:
            print("No updates")
        
        return True

    def load_researchers(self):
        print("Inserting researchers...")
        with self.driver.session() as session:
            result = session.run("""
                USING PERIODIC COMMIT 1000
                LOAD CSV WITH HEADERS FROM 'file:///output_author.csv' AS row 
                FIELDTERMINATOR ';'
                WITH row WHERE row.ID IS NOT NULL 
                MERGE (a:Researcher { authorID:row.ID, name:row.author })
                """)
            
            if self.printResult(result):
                print("Researchers inserted")
            else:
                print("Error inserting authors")

    def load_conference(self):
        print("Inserting conferences...")
        with self.driver.session() as session:
            result = session.run("""
                USING PERIODIC COMMIT 1000
                LOAD CSV WITH HEADERS FROM 'file:///output_conferences_2.csv' AS row 
                FIELDTERMINATOR ';'
                WITH row WHERE row.ID IS NOT NULL 
                MERGE (a:Author { authorID:row.ID, name:row.author })
                """)
            
            if self.printResult(result):
                print("Authors inserted")
            else:
                print("Error inserting authors")
        

    def clean_all(self):
        print("Cleaning database...")
        with self.driver.session() as session:
            result = session.run("""
                MATCH (n) DELETE n
                """)
            
        if self.printResult(result, "clean"):
            print("Database cleaned")
        else:
            print("Error cleaning the database")
        
        
