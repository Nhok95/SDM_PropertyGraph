from neo4j import GraphDatabase
import argparse

community_keywords = ["Data Management", "Indexing", "Data Modeling", "Big Data", 
"Data Processing", "Data Storage", "Data Querying"]

def DataBaseCommunity(dbkeywords):
	with driver.session() as session:
		session.run("""
			MATCH (n)-[:PROCEEDING|:VOLUME]->(p:Paper)
			WITH n.title AS Title, count(p.title) AS TotalPapers
			MATCH (n2)-[:PROCEEDING|:VOLUME]->(p2:Paper)-->(k:Keyword)
			WHERE k.topic IN $dbkeywords
			WITH n2.title AS Title2, Title, count(p2.title) AS ComPapers, TotalPapers, collect(p2.title) AS Papers
			WHERE Title2 = Title AND toFloat(toFloat(ComPapers)/toFloat(TotalPapers)) >= 0.9
			UNWIND Papers AS p
			MERGE (c:DBPaper {title: p})
		""", dbkeywords=dbkeywords)
		

def TOP100DBPapers():
	with driver.session() as session:

		session.run("""
			CALL gds.graph.create.cypher(
				'myGraph',
				'MATCH (n:Paper) RETURN id(n) AS id',
				'MATCH (n:Paper)-[:CITES]->(m:Paper)
				 RETURN id(n) AS source, id(m) AS target'
			)
		""")

		papers = session.run("""
			MATCH (d:DBPaper)
			RETURN d.title
		""").data()

		dbpapers = []

		for i in range(0, len(papers)):
			dbpapers.append(papers[i].get('d.title'))

		#print(dbpapers)

		top = session.run("""
			CALL gds.pageRank.stream('myGraph', {maxIterations:5})
			YIELD nodeId, score
			WITH gds.util.asNode(nodeId).title AS Title, score
			WHERE Title IN $papers
			RETURN Title, score
			ORDER BY score DESC LIMIT 100
		""", papers=dbpapers).data()


		top100 = []

		for i in range(0, len(top)):
			top100.append(top[i].get('Title'))


		#print(top100)

		session.run("""
			UNWIND $top AS t100
			MERGE (t:Top100 {title:t100})
		""", top=top100)



def Gurus():
	with driver.session() as session:

		top = session.run("""
			MATCH (t:Top100)
			RETURN t.title
		""").data()

		top100 = []

		for i in range(0, len(top)):
			top100.append(top[i].get('t.title'))

		guru = session.run("""
			MATCH (r:Researcher)<-[:AUTHOR*2..]-(p:Paper)
			WHERE p.title IN $top
			WITH r.name as Guru
			RETURN Guru
			""", top=top100).data()

		gurus = []

		for i in range(0, len(guru)):
			gurus.append(guru[i].get('Guru'))

		print("Gurus of the community: ", gurus)

def GetParserArgs():
    parser = argparse.ArgumentParser()
    parser.add_argument("-m", "--mode", default='load',
                        help="Select mode", 
                        type=str, choices=['load','clean'])
    parser.add_argument("-u", "--user", default='neo4j',
                        help="username", 
                        type=str)
    parser.add_argument("-p", "--password",
                        help="password", 
                        type=str)

    return parser.parse_args()



if __name__ == "__main__":
    args = GetParserArgs()

    if args.password == None:
        driver = GraphDatabase.driver("bolt://localhost:7687")
    else:
        driver = GraphDatabase.driver("bolt://localhost:7687", (args.user, args.password))

    DataBaseCommunity(community_keywords)
    TOP100DBPapers()
    Gurus()