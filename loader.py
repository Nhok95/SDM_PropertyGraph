from neo4j import GraphDatabase

class Neo4JLoader:

    def __init__(self, uri, auth_value=None):

        print("auth: {}".format(auth_value))
        self.driver = GraphDatabase.driver(uri, auth=auth_value)

    def close(self):

        print("##########################")
        print("Connection closed")
        self.driver.close()
    
    @staticmethod
    def printResult(result):

        print("----------------------")
        counter = result.consume().counters
        resultStr = ''
        if counter.contains_updates:
            if counter.labels_added > 0:
                resultStr += 'Added {0} labels. '.format(counter.labels_added)
            if counter.labels_removed > 0:
                resultStr += 'Removed {0} labels. '.format(counter.labels_removed)

            if counter.nodes_created > 0:
                resultStr += 'Created {0} nodes. '.format(counter.nodes_created)
            if counter.nodes_deleted > 0:
                resultStr += 'Deleted {0} nodes. '.format(counter.nodes_deleted)

            if counter.relationships_created > 0:
                resultStr += 'Created {0} relationships. '.format(counter.relationships_created)
            if counter.relationships_deleted > 0:
                resultStr += 'Deleted {0} relationships. '.format(counter.relationships_deleted)

            if counter.properties_set > 0:
                resultStr += 'Set {0} properties. '.format(counter.properties_set)

            if counter.indexes_added > 0:
                resultStr += 'Added {0} indexes. '.format(counter.indexes_added)
            if counter.indexes_removed > 0:
                resultStr += 'Removed {0} indexes. '.format(counter.indexes_removed)
            
            if counter.constraints_added > 0:
                resultStr += 'Added {0} constraints. '.format(counter.constraints_added)
            if counter.constraints_removed > 0:
                resultStr += 'Removed {0} constraints. '.format(counter.constraints_removed)

            print(resultStr)
            
        else:
            print("No updates")
        
        print("----------------------")
        
        return True
        
    def load_journals_articles(self):
        print("##########################")
        print("Inserting journals and articles...")
        with self.driver.session() as session:
            result = session.run("""
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

            if self.printResult(result):
                print("Journals and articles inserted")
            else:
                print("Error inserting journals and articles")

    def load_authors_articles(self):
        print("##########################")
        print("Inserting authors for articles")
        with self.driver.session() as session:
            result = session.run("""
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
        
            if self.printResult(result):
                print("Authors for articles inserted")
            else:
                print("Error inserting authors for articles")

    def load_paper_citations(self):
        print("##########################")
        print("Inserting citations for papers")
        with self.driver.session() as session:
            result = session.run("""
                MATCH (p:Paper)
                WITH p
                MATCH (c:Paper) 
                WHERE p <> c AND rand() < 0.1
                MERGE (p)-[:CITES]->(c)
            """)
        
            if self.printResult(result):
                print("Citations for papers inserted")
            else:
                print("Error inserting authors for articles")

    def clean_all(self):
        print("##########################")
        print("Cleaning database...")
        with self.driver.session() as session:
            result1 = session.run("""
                MATCH (n)-[r]-() DELETE n,r
                """)
        
            if self.printResult(result1):
                print("Deleted nodes and its relations")
            else:
                print("Error cleaning the database")

        with self.driver.session() as session:
            result2 = session.run("""
                MATCH (n) DELETE n
                """)
            if self.printResult(result2):
                print("Database cleaned")
            else:
                print("Error cleaning the database")
        

        
