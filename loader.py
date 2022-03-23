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
    
    ### TRANSACTION FUNCTIONS ###
    @classmethod
    def author_transaction(cls, tx, file):
        return tx.run("""
            LOAD CSV WITH HEADERS FROM $file AS row
            FIELDTERMINATOR ';'
            WITH row WHERE row.key IS NOT NULL AND row.title IS NOT NULL
            MERGE (p:Paper { paperID: row.key, title: row.title, abstract: row.abstract})
            WITH row, p, SPLIT(row.author, '|') AS author
            MERGE (s:Scientist { name: author[0]}) 
            MERGE (p)-[:AUTHOR]->(s)
            WITH row, p, SPLIT(row.author, '|') AS author
            UNWIND RANGE(1,SIZE(author)-1) as i
            MERGE (p)-[:COAUTHOR]->(s:Scientist { name: author[i]}) 
            """, file=file)

    @classmethod
    def review_transaction(cls, tx, file):
        return tx.run("""
            LOAD CSV WITH HEADERS FROM $file AS row
            FIELDTERMINATOR ';'
            MATCH (p:Paper { paperID: row.key, title: row.title, abstract: row.abstract})
            WITH row, p
            UNWIND SPLIT(row.reviewers, '|') AS reviewer
            MATCH (s:Scientist { name: reviewer})
            CREATE (p)<-[r:REVIEWS]-(s)
            SET r.text=row.review, r.decision=row.decision
            """, file=file)
    
    @classmethod
    def organization_transaction(cls, tx, file):
        return tx.run("""
            LOAD CSV WITH HEADERS FROM $file AS row
            FIELDTERMINATOR ';'
            WITH row WHERE row.key IS NOT NULL AND row.title IS NOT NULL
            MERGE (o:Organization { name:row.organization, type:row.type})
            WITH row, o
            UNWIND SPLIT(row.author, '|') AS author
            MATCH (s:Scientist { name: author})
            CREATE (s)-[:AFFILIATED]->(o)
            """, file=file)

    ### LOAD FUNCTIONS ###

    ## Journals ##
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

    def load_journals_authors_articles(self):
        print("##########################")
        print("Inserting authors for articles (journals)...")
        with self.driver.session() as session:
            result = session.write_transaction(self.author_transaction, 'file:///journals_extracted.csv')
        
            if self.printResult(result):
                print("Authors for articles inserted (from Journals)")
            else:
                print("Error inserting authors for articles") 

    #############

    ## Conferences ##
    def load_conference_articles(self):
        print("##########################")
        print("Inserting conferences and articles...")
        with self.driver.session() as session:
            result = session.run("""
                LOAD CSV WITH HEADERS FROM 'file:///conferences_extracted.csv' AS row
                FIELDTERMINATOR ';'
                WITH row WHERE row.key IS NOT NULL AND row.title IS NOT NULL
                MERGE (c:Conference { title: row.booktitle, year: row.year})
                MERGE (p:Paper { paperID: row.key, title: row.title, abstract: row.abstract})
                MERGE (y:Year { value: toString(row.year)})
                MERGE (c)-[pr:PROCEEDING]->(p)
                WITH row, p
                UNWIND SPLIT(row.keywords, '|') AS keyword
                MERGE (k:Keyword { topic: keyword})
                MERGE (p)-[:TOPIC]-(k)
                """)

            if self.printResult(result):
                print("Conferences and articles inserted")
            else:
                print("Error inserting conferences and articles")

    def load_conference_authors_articles(self):
        print("##########################")
        print("Inserting authors for articles (conferences)...")
        with self.driver.session() as session:
            result = session.write_transaction(self.author_transaction, 'file:///conferences_extracted.csv')

            if self.printResult(result):
                print("Authors for articles inserted (from Conferences)")
            else:
                print("Error inserting authors for articles")

    def load_conference_cities(self):
        print("##########################")
        print("Inserting conference cities...")
        with self.driver.session() as session:
            result = session.run("""
                LOAD CSV WITH HEADERS FROM 'file:///conferences_extracted2.csv' AS row
                FIELDTERMINATOR ';'
                WITH row WHERE row.booktitle IS NOT NULL
                MERGE (c:Conference { title: row.booktitle, year:row.year})
                MERGE (y:Year { year:row.year})
                MERGE (ct:City { cityName:row.city, country:row.country})
                MERGE (c)-[:CELEBRATED_YEAR]-(y)
                MERGE (c)-[:CELEBRATED_CITY]-(ct)
                WITH row, c
                SET c.edition = row.edition
                """)

            if self.printResult(result):
                print("Conference cities inserted")
            else:
                print("Error inserting conference cities")
    
    #############

    ## Reviews ##
    def load_article_reviews(self):
        print("##########################")
        print("Inserting reviews...")
        with self.driver.session() as session_a:
            result = session_a.write_transaction(self.review_transaction, 'file:///journals_extracted.csv')

            if self.printResult(result):
                print("Article reviews inserted (from Journals)")
            else:
                print("Error inserting article reviews")

        with self.driver.session() as session_b:
            result = session_b.write_transaction(self.review_transaction, 'file:///conferences_extracted.csv')

            if self.printResult(result):
                print("Article reviews inserted (from Conferences)")
            else:
                print("Error inserting article reviews")

    #############

    ## Organizations ##
    def load_organizations(self):
        print("##########################")
        print("Inserting organizations...")
        with self.driver.session() as session_a:
            result = session_a.write_transaction(self.organization_transaction, 'file:///journals_extracted.csv')
            
            if self.printResult(result):
                print("Organizations inserted (from Journals)")
            else:
                print("Error inserting organizations")
        
        with self.driver.session() as session_b:
            result = session_b.write_transaction(self.organization_transaction, 'file:///conferences_extracted.csv')

            if self.printResult(result):
                print("Organizations inserted (from Conferences)")
            else:
                print("Error inserting organizations")


    #############

    ## Cites ##
    def load_paper_citations(self):
        print("##########################")
        print("Inserting citations for papers...")
        with self.driver.session() as session:
            result = session.run("""
                MATCH (p:Paper)
                WITH p
                MATCH (c:Paper) 
                WHERE p <> c AND rand() < 0.05
                CREATE (p)-[:CITES]->(c)
            """)
        
            if self.printResult(result):
                print("Citations for papers inserted")
            else:
                print("Error inserting citations")

    #############

    ## Clean BD ##
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
    
    #############