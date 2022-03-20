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
                WITH row WHERE row.key IS NOT NULL AND row.title IS NOT NULL
                MERGE (j:Journal { title: row.journal, year: row.year})
                MERGE (p:Paper { paperID: row.key, title: row.title, abstract: row.abstract})
                MERGE (y:Year { value: toString(row.year)})
                MERGE (j)-[v:VOLUME]->(p)
                SET v.vol=row.volume
                MERGE (j)-[:PUBLISHED]-(y)
                WITH row, p
                UNWIND SPLIT(row.keywords, '|') AS keyword
                MERGE (k:Keyword { topic: keyword})
                MERGE (p)-[:TOPIC]-(k)
                """)
        print("Journals and articles inserted")

    def load_authors_articles(self):
        print("Inserting authors for articles")
        with self.driver.session() as session:
            session.run("""
                LOAD CSV WITH HEADERS FROM 'file:///journals_extracted.csv' AS row
                FIELDTERMINATOR ';'
                WITH row WHERE row.key IS NOT NULL AND row.title IS NOT NULL
                MERGE (p:Paper { paperID: row.key, title: row.title, abstract: row.abstract})
                WITH row, p, SPLIT(row.author, '|') AS author 
                MERGE (a:Author { name: author[0]})
                MERGE (p)-[:AUTHOR]->(a) 
                WITH row, p, SPLIT(row.author, '|') AS author 
                UNWIND RANGE(1,SIZE(author)-1) as i
                MERGE (p)-[:COAUTHOR]->(a:Author { name: author[i]})
                """)
        print("Authors for articles inserted")

    def clean_all(self):
        print("Cleaning database...")
        with self.driver.session() as session:
            session.run("""
                MATCH (n)-[r]-() DELETE n,r
                """)

        with self.driver.session() as session:
            session.run("""
                MATCH (n) DELETE n
                """)
        
        print("Database cleaned")

