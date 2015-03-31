import csv
import itertools
import networkx as nx
from mediacloud.storage import MongoStoryDatabase

class CustomStoryDatabase(MongoStoryDatabase):

    def getStories(self, query):
        """
        Just gets stories based on a query, fun.
        """
        return self._db.stories.find( query )

    def getEdges(self, query=None):
        """
        Gets all the edges (i.e. links) in the graph scoped by the given query.

        :param query: Mongo query.
        :return: Iterable of tuples, e.g. (`http://source_url.com`, `http://target_url.com`)
        """
        if query is None:
            query = {}
        stories = self.getStories(query)
        graph = self.buildGraph(stories)
        return graph.edges()

    def buildGraph(self, stories, inlinks_only=False):
        """
        Builds a networkx graph from a list of stories.

        :param stories: iterable of Media Cloud stories.
        :param inlinks_only: set to True to prevent links from other domains from being part of the graph.
        """
        graph = nx.DiGraph()
        for story in stories:
            node = story['guid']
            graph.add_node(node, story)
            for link in story['story_links']:
                if inlinks_only is True and link['inlink'] is False:
                    continue
                linknode = link['href']
                graph.add_node(linknode)
                graph.add_edge(node, linknode, link)
        return graph

    def getCocitations(self, url, query={}):
        """
        Gets all of the cocitations of a URL.
        Cocitations are all of the successors of a URL's predecessors.

        :param url: The URL in the database.
        :param query: Limit the graph of the query.
        """
        stories = self.getStories(query)
        graph = self.buildGraph(stories, inlinks_only=True)
        cocites = set()
        for predecessor in graph.predecessors_iter(url):
            cocites |= set(graph.successors(predecessor))
        cocites -= set([url])
        return [graph.node[c] or {'href': c} for c in cocites]

    def getSpiderCommunity(self, url, query={}, spider=0):
        """
        Builds a list of links around a given URL and ranks them by in_degree.

        :param url: The URL in the database.
        :param query: Limit the query in the graph.
        :param spider: How many levels to spider out from the original URL.
        :return: List of links, ordered by relevancy.
        """
        subgraph = self.getSpiderSubgraph(url, query, spider)
        stories = sorted(subgraph.in_degree_iter(subgraph.nodes()), key=lambda s: s[1], reverse=True)
        return [subgraph.node[s[0]] or {'href': s[0]} for s in stories if s[0] != url]

    def getSpiderSubgraph(self, url, query={}, spider=1):
        """
        Builds a graph by spidering out from a given URL and including all articles.

        :param url: The URL in the database to build a subgraph around.
        :param query: Limit the scope of the graph.
        :param spider: How many levels to spider out from the original URL.
        :return: networkx graph with the spidered links.
        """
        stories = self.getStories(query)
        graph = self.buildGraph(stories, inlinks_only=True)
        urls = [url]
        all_results = set()
        for i in range(spider):
            new_results = set()
            for url in urls:
                neighbors = set([n for n in nx.all_neighbors(graph, url)])
                new_results |= (neighbors - all_results)
                all_results |= neighbors
            urls = list(new_results)
        return graph.subgraph(all_results)

    def getNeighbors(self, url, query={}):
        """
        Gets all the details about the links to and from a given URL.

        :param url: The URL in the database.
        :param query: Limit the scope of the graph.
        :return: List of links with detailed metadata.
        """
        stories = self.getStories(query)
        graph = self.buildGraph(stories, inlinks_only=True)
        if url not in graph:
            return []

        res = []

        chain = itertools.chain.from_iterable([
            graph.in_edges_iter(url,data=True),
            graph.out_edges_iter(url,data=True)])

        for src, target, data in chain:
            if src == url:
                # It is an out edge
                data['is_in_edge'] = False
                target_node = target
            else:
                # It is an in edge
                data['is_in_edge'] = True
                target_node = src
            details = graph.node[target_node] if target_node in graph else {}
            data.update(details)
            data['href'] = target_node
            res.append(data)
        return res

    # def mapR(self, query):
    #     from bson.code import Code
    #     m = Code("""function() {
    #             this.story_links.forEach(function(z) {
    #                 emit(this.guid, z.href);
    #             });
    #         }""")
    #     r = Code("""function(key, values) {
    #             return values.length;
    #         }""")
    #     result = self._db.stories.map_reduce(m, r, 'mclinks')
    #     return result

    def getStoryLinkData(self, query={}, with_metadata=True):
        """
        Get high-level data about the linking behavior of stories in a given query.

        :param query: Limit the data analysis to the stories matching the given query.
        :param with_metadata: Get meta information about the links along with the stories.
        :return: List or dict, depending on whether `with_metadata` is enabled.
        """
        stories = self._db.stories.aggregate([
                {'$match': query},
                {'$project': {
                    'guid': 1,
                    'hrefs': '$story_links.href',
                    'num_links': {'$size': '$story_links'},
                    # I can't get this to actually work; it just returns a list of 1s and 0s, and you need to sum it afterwards
                    # I probably need to use $unwind
                    'num_inlinks': {'$map': {'input': '$story_links', 'as': 'link', 'in': {'$cond': ['$$link.inlink',1,0]}}},
                    'wordcount': 1,
                    'grafcount': 1
                }},
                {'$sort': {'_id': 1}}
            ])['result']
        # Fix the problem here

        if not stories:
            return None

        for i in range(len(stories)):
            stories[i]['num_inlinks'] = sum(stories[i]['num_inlinks'])

        if with_metadata is not True:
            return stories

        # Get results-wide metadata about the query (not just story-wide)

        linkdata = [s['num_links'] for s in stories]
        inlinkdata = [s['num_inlinks'] for s in stories]
        wordcountdata = [s['wordcount'] for s in stories]

        num_with_links = len(filter(lambda s: s['num_links']>0, stories))
        num_with_inlinks = len(filter(lambda s: s['num_inlinks']>0, stories))

        def median(l):
            half = len(l) / 2
            l.sort()
            if len(l) % 2 == 0:
                return (l[half-1] + l[half]) / 2.0
            else:
                return l[half]

        data = {'stories': stories}
        data['meta'] = {
            'numStories': len(stories),
            'avgLinks': float(sum(linkdata)) / len(linkdata),
            'avgInlinks': float(sum(inlinkdata)) / len(inlinkdata),
            'avgWordcount': float(sum(wordcountdata)) / len(wordcountdata),
            'medianLinks': median(linkdata),
            'medianInlinks': median(inlinkdata),
            'maxLinks': max(linkdata),
            'numWithLinks': num_with_links,
            'numWithInlinks': num_with_inlinks,
            'numLinks': sum(linkdata),
            'numInlinks': sum(inlinkdata),
            'pctWithLinks': float(num_with_links) / len(stories),
            'pctWithInlinks': float(num_with_inlinks) / len(stories)
        }
        return data



class CsvQuerier(object):

    def __init__(self, db_name='mclinks', media_id=None, topic_urls=None, patterns=None, use_guid=True, **kwargs):
        self.db = CustomStoryDatabase(db_name)
        self.media_id = media_id
        self.topic_urls = topic_urls or ()
        self.patterns = patterns or ()
        self.use_guid = use_guid

    def getNodeRows(self, query):
        return [{
            '_id': s['_id'],
            'guid': s['guid'],
            'url': s['url'],
            'stories_id': s['stories_id'],
            'media_id': s['media_id'],
            'title': s['title'].encode('ascii', 'ignore'),
            'pubdate': s['publish_date'],
            'num_links': len(s['story_links']),
            'num_inlinks': len([l for l in s['story_links'] if l['inlink'] is True]),
            'hrefs': ', '.join([i['href'] for i in s['story_links']])
        } for s in self.db.getStories(query)]

    def getEdgeRows(self, query=None):
        edges = self.db.getEdges(query)
        rows = [{
            'source': edge[0],
            'target': edge[1]
        } for edge in edges]
        return rows

    def getPatternData(self):
        rows = []
        query = {}
        if self.media_id is not None:
            query['media_id'] = self.media_id
        for pattern in self.patterns:
            if self.use_guid:
                query['guid'] = {'$regex': pattern}
            else:
                query['url'] = {'$regex': pattern}
            res = self.db.getStoryLinkData(query)

            if res is None:
                print 'No stories found for pattern %s' % pattern
                continue
            
            res = self.addCustomData(res)

            for k,v in res['meta'].items():
                res[k] = v
            res.pop('meta')
            res.pop('stories')

            res['pattern'] = pattern
            rows.append(res)
        return rows

    def addCustomData(self, res):
        hrefs = [a['hrefs'] for a in res['stories']]
        total_count = 0
        topics_count = 0
        for href in hrefs:
            for url in href:
                total_count += 1
                if any(i in url for i in self.topic_urls):
                    topics_count += 1
        res['numTopicLinks'] = topics_count
        res['pctTopicLinks'] = float(topics_count) / total_count if total_count != 0 else 0.0
        return res

    def getDictFromCsv(self, infile):
        """
        """
        with open(infile, 'r') as f:
            reader = csv.reader(f)
            headers = reader.next()
            nodes = []
            for row in reader:
                node = {}
                for index, item in enumerate(row):
                    node[headers[index]] = item
                nodes.append(node)
        return nodes

    def customizeStoryData(self, infile='out/spider_links_mod.csv', id_field='id'):
        """
        """
        nodes = self.getDictFromCsv(infile)
        node_dict = { node[id_field]: node for node in nodes }
        stories = self.db.getStories({})
        s = []
        for story in stories:
            if story['guid'] in node_dict:
                copy = story.copy()
                copy.update(node_dict[story['guid']])
                s.append(copy)
        return s
