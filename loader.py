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
        
    def load_journals_articles(self):
        print("Inserting journals and articles...")
        with self.driver.session() as session:
            session.run("""
                LOAD CSV WITH HEADERS FROM 'file:///journals_extracted.csv' AS row
                FIELDTERMINATOR ';'
                WITH row WHERE row.ID IS NOT NULL
                MERGE (j:Journal { journalID: row.key, title: row.journal})
                MERGE (p:Paper { paperID: row.ID, title: row.title, abstract: row.abstract})
                MERGE (y:Year { value: toString(row.year)})
                MERGE (j)-[v:VOLUME]->(p)
                SET v.vol=row.volume
                MERGE (j)-[:PUBLISHED_IN]-(y)
                """)
        print("Journals and articles inserted")

    def clean_all(self):
        print("Cleaning database...")
        with self.driver.session() as session:
            session.run("""
                MATCH (n) DELETE n
                """)
        
        print("Database cleaned")
