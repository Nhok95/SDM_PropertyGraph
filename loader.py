from neo4j import GraphDatabase

class Neo4JLoader:

    def __init__(self, uri, auth_value=None):

        print("auth: {}".format(auth_value))
        self.driver = GraphDatabase.driver(uri, auth=auth_value)

    def close(self):

        print("connection closed")
        self.driver.close()

    def load_authors(self):
        print("Inserting authors...")
        with self.driver.session() as session:
            session.run("""
                LOAD CSV WITH HEADERS FROM 'file:///output_author_2.csv' AS row 
                FIELDTERMINATOR ';'
                WITH row WHERE row.ID IS NOT NULL 
                MERGE (a:Author { authorID:row.ID, name:row.author })
                """)
        
        print("Authors inserted")
        

    def clean_all(self):
        print("Cleaning database...")
        with self.driver.session() as session:
            session.run("""
                MATCH (n) DELETE n
                """)
        
        print("Database cleaned")
