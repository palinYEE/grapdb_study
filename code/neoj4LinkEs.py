from elasticsearch import Elasticsearch
from neo4j import GraphDatabase
from datetime import datetime

# onion data mapping
def inputOnionData(greeter, onion, profiling):
    greeter.run("MERGE (a:Onion {onion: $onion , profiling: $profiling})", onion=onion, profiling= profiling)

# google data mapping
def inputGoogleData(greeter, timestamp, abstract, keyword, surfaceLink, title):
    greeter.run("MERGE (b:Google {timestamp: $timestamp, abstract: $abstract, keyword: $keyword, surfaceLink: $surfaceLink, title: $title})", timestamp=timestamp, abstract=abstract, keyword=keyword, title=title)

# link between onion site and profiling keyword
def addOnion(greeter):
    greeter.run("MATCH (a:Onion) "
        "MERGE (b:Profiling {name:a.profiling}) "
        "MERGE (a)-[r:Onion]->(b)")

# link between surface site and profilng keyword
def addSurface(greeter):
    greeter.run("MATCH (a:Google) "
        "MERGE (b:Profiling {name:a.keyword}) "
        "MERGE (a)<-[r:Surface]-(b)")


# Elasticsearch method
class Elastic():
    def init(self, ip, port, index):
        self.ip = ip
        self.port = port
        self.indexName = index

        self.es = Elasticsearch(f"{ip}:{port}")

    def searchData(self, query, size):
        if size > 1000:
            return self.es.search(index = self.indexName, body = query, size = size, scroll = '1m')
        else:
            return self.es.search(index = self.indexName, body = query, size = size)

    def scrollData(self, scrollId):
        return self.es.scroll(scroll_id = scrollId, scroll='1m')

    def clearScroll(self, scrollIdList):
        for scrollId in set(scrollIdList):
            self.es.clear_scroll(scroll_id = scrollId)

# neo4j method
class NEO4J():
    def init(self, ip, port, id, password):
        self.greeter = GraphDatabase.driver(f"neo4j://{ip}:{port}", auth = (id, password))

    def inputOnionDataNeo4j(self, dataList):
        with self.greeter.session() as session:
            for data in dataList:
                    printLog(f"{data['onion']} and {data['profiling']}")
                    session.write_transaction(inputOnionData,
                                              onion = data['onion'],
                                              profiling = data['profiling'])




    def inputGoogleDataNeso4j(self, dataList):
        with self.greeter.session() as session:
            for data in dataList:
                printLog(f"{data['surfaceLink']} and {data['keyword']['value']}")
                session.write_transaction(inputGoogleData,
                                          timestamp = data['@timestamp'],
                                          abstract = data['abstract'],
                                          keyword = data['keyword']['value'],
                                          surfaceLink = data['surfaceLink'],
                                          title = data['title'])

# onion data preprocessing 
def onionDataPrepro(data):
    resultData = {}
    profilingList = []
    resultData['onion'] = data['_source']['requestURL']
    try:
        resultData['profiling'] = data['_source']['profiling']
    except KeyError:
        resultData['profiling'] = data['_source']['profilling']

    except Exception as e:
        print(f"[onionDataPrepro][ERROR] - {e}")
        return {}
    
    for key in resultData['profiling'].keys():
        profilingList.extend(resultData['profiling'][key])
    
    resultData['profiling'] = profilingList

    return resultData

# google data preprocessing
def googleDataPrepro(data):
    return data['_source']

# collecting data at elasticsearch
def dataCollection():
    onionES = Elastic(ip = '<ip>', port = '<port>', index = '<index name>')
    googleES = Elastic(ip = '<>', port = '<port>', index = '<index name>')
    
    onionEsQuery = {
        "_source" : [
            "requestURL",
            "profilling",
            "profiling"
        ],
        "query":{
            "match_all" : {}
        }
    }
    googleEsQuery = {
        "query":{
            "match_all" : {}
        }
    }

    onionESdataList = []
    googleESdataList = []

    onionESclearScrollIdList = []
    googleESclearScrollIdList = []

    onionData = onionES.searchData(query = onionEsQuery, size = 5000)
    googleData = googleES.searchData(query = googleEsQuery, size = 5000)

    onionESdataList.extend(map(onionDataPrepro, onionData['hits']['hits']))
    onionScrollId = onionData.get('_scroll_id')

    googleESdataList.extend(map(googleDataPrepro, googleData['hits']['hits']))
    googleScrollId = googleData.get('_scroll_id')

    while onionScrollId:
        onionESclearScrollIdList.append(onionScrollId)
        onionData = onionES.scrollData(scrollId = onionScrollId)
        onionESdataList.extend(map(onionDataPrepro, onionData['hits']['hits']))
        onionScrollId = onionData.get('_scroll_id')


    while googleScrollId:
        googleESclearScrollIdList.append(googleScrollId)
        googleData = googleES.scrollData(scrollId = googleScrollId)
        googleESdataList.extend(map(googleDataPrepro, googleData['hits']['hits']))
        googleScrollId = googleData.get('_scroll_id')


    onionES.clearScroll(onionESclearScrollIdList)
    googleES.clearScroll(googleESclearScrollIdList)

    return onionESdataList, googleESdataList

def printLog(message):
    print(f"[{str(datetime.now())}] {message}")

if name == 'main':
    printLog("TEST NEO4J START")
    testNeo = NEO4J(ip='<ip>', port='<port>', id = '<id>', password = '<password>')

    onionDataList, googleDataList = dataCollection()

    printLog(f"onionDataList : {len(onionDataList)}")
    printLog(f"googleDataList : {len(googleDataList)}")
    
    printLog("START NEO4J DATA INSERT")
    testNeo.inputOnionDataNeo4j(onionDataList)
    printLog("SUCCESS ONION DATA INSERT")

    testNeo.inputGoogleDataNeo4j(googleDataList)
    printLog("SUCCESS GOOGLE DATA INSERT")