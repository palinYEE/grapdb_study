from elasticsearch import Elasticsearch 
from neo4j import GraphDatabase
from datetime import datetime

def inputOnionData(greeter, onion, profiling):
    greeter.run("MERGE (a:Onion {onion: $onion , profiling: $profiling})", onion=onion, profiling= profiling)

def inputGoogleData(greeter, timestamp, abstract, keyword, surfaceLink, title):
    greeter.run("MERGE (b:Google {timestamp: $timestamp, abstract: $abstract, keyword: $keyword, surfaceLink: $surfaceLink, title: $title})", timestamp=timestamp, abstract=abstract, keyword=keyword, surfaceLink = surfaceLink, title=title)

def addOnion(greeter):
    greeter.run("MATCH (a:Onion) "
        "MERGE (b:Profiling {name:a.profiling}) "
        "MERGE (a)-[r:Onion]->(b)")

def addSurface(greeter):
    greeter.run("MATCH (a:Google) "
        "MERGE (b:Profiling {name:a.keyword}) "
        "MERGE (a)<-[r:Surface]-(b)")


class Elastic():
    def __init__(self, ip, port, index):
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
        return self.es.scroll(scroll_id = scrollId, scroll='2m')

    def clearScroll(self, scrollIdList):
        for scrollId in set(scrollIdList):
            self.es.clear_scroll(scroll_id = scrollId)


class NEO4J():
    def __init__(self, ip, port, id, password):
        self.greeter = GraphDatabase.driver(f"neo4j://{ip}:{port}", auth = (id, password))

    def inputOnionDataNeo4j(self, dataList):
        with self.greeter.session()  as session:
            for data in dataList:
                    printLog(f"{data['onion']} and {data['profiling']}")
                    session.write_transaction(inputOnionData, 
                                              onion = data['onion'], 
                                              profiling = data['profiling'])




    def inputGoogleDataNeo4j(self, dataList):
        with self.greeter.session() as session:
            for data in dataList:
                printLog(f"{data['surfaceLink']} and {data['keyword']['value']}")
                session.write_transaction(inputGoogleData,
                                          timestamp     = data['@timestamp'],
                                          abstract      = data['abstract'],
                                          keyword       = data['keyword']['value'],
                                          surfaceLink   = data['surfaceLink'],
                                          title         = data['title'])


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
        return 
    
    for key in resultData['profiling'].keys():
        profilingList.extend(resultData['profiling'][key])
    
    if not profilingList:
        return 

    resultData['profiling'] = profilingList

    return resultData

def googleDataPrepro(data):
    return data['_source']

def dataCollection():
    '''elasticsearch에서 데이터 가져오는 함수'''
    onionES = Elastic(ip = '', port = '', index = '')
    googleES = Elastic(ip = '', port = '', index = '')
    
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
    onionESdataList = list(filter(None, onionESdataList))
    onionScrollId = onionData.get('_scroll_id')

    googleESdataList.extend(map(googleDataPrepro, googleData['hits']['hits']))
    googleScrollId = googleData.get('_scroll_id')

    # while onionScrollId:
    #     onionESclearScrollIdList.append(onionScrollId)
    #     onionData = onionES.scrollData(scrollId = onionScrollId)
    #     onionESdataList.extend(map(onionDataPrepro, onionData['hits']['hits']))
    #     onionScrollId = onionData.get('_scroll_id')
    #     if not onionData['hits']['hits']: break


    # while googleScrollId:
    #     printLog(f"{googleData['hits']['hits']}")
    #     googleESclearScrollIdList.append(googleScrollId)
    #     googleData = googleES.scrollData(scrollId = googleScrollId)
    #     googleESdataList.extend(map(googleDataPrepro, googleData['hits']['hits']))
    #     googleScrollId = googleData.get('_scroll_id')
    #     if not googleData['hits']['hits']: break


    onionES.clearScroll(onionESclearScrollIdList)
    googleES.clearScroll(googleESclearScrollIdList)

    return onionESdataList, googleESdataList

def printLog(message):
    print(f"[{str(datetime.now())}] {message}")

if __name__ == '__main__':
    printLog("TEST NEO4J START")
    testNeo = NEO4J(ip='', port='', id = '', password = '')

    onionDataList, googleDataList = dataCollection()

    printLog(f"onionDataList  : {len(onionDataList)}")
    printLog(f"googleDataList : {len(googleDataList)}")
    
    printLog("START NEO4J DATA INSERT")
    testNeo.inputOnionDataNeo4j(onionDataList)
    printLog("SUCCESS ONION DATA INSERT")
    testNeo.inputGoogleDataNeo4j(googleDataList)
    printLog("SUCCESS GOOGLE DATA INSERT")
