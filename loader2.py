from neo4j import GraphDatabase

class Neo4JLoader2:

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
    
    def load_authors_articles2(self):
        print("##########################")
        print("Inserting conferences and articles 2...")
        with self.driver.session() as session:
            result = session.run("""
                LOAD CSV WITH HEADERS FROM 'file:///conferences_extracted.csv' AS row
                FIELDTERMINATOR ';'
                WITH row WHERE row.key IS NOT NULL AND row.title IS NOT NULL
                MERGE (p:Paper { paperID: row.key, title: row.title, abstract: row.abstract})
                WITH row, p, SPLIT(row.author, '|') AS author
                MERGE (s:Scientist { name: author[0]}) 
                MERGE (p)-[:AUTHOR]->(s)
                WITH row, p, SPLIT(row.author, '|') AS author
                UNWIND RANGE(1,SIZE(author)-1) as i
                MERGE (p)-[:COAUTHOR]->(s:Scientist { name: author[i]}) 
                """)

            if self.printResult(result):
                print("Authors for articles inserted")
            else:
                print("Error inserting authors for articles")

    def load_conference_cities(self):
        print("##########################")
        print("Inserting conference cities...")
        with self.driver.session() as session:
            #Edition period?
            result = session.run("""
                LOAD CSV WITH HEADERS FROM 'file:///conferences_extracted2.csv' AS row
                FIELDTERMINATOR ';'
                WITH row WHERE row.key IS NOT NULL AND row.title IS NOT NULL
                MERGE (e:Edition { name: row.edition})
                MERGE (c:Conference { title: row.booktitle})
                MERGE (y:Year { year:row.year})
                MERGE (ct:City { city:row.city, country:row.country})
                MERGE (c)-[:EDITION]-(e)
                MERGE (e)-[:CELEBRATED_YEAR]-(y)
                MERGE (e)-[:CELEBRATED_CITY]-(ct)
                """)

            if self.printResult(result):
                print("Conference cities inserted")
            else:
                print("Error inserting conference cities")
        
