from mediacloud.storage import MongoStoryDatabase

class CustomStoryDatabase(MongoStoryDatabase):

    def getStories(self, query):
        return self._db.stories.find( query )

    def getEdges(self, query):
        stories = self._db.stories.find( query )
        nodes = []
        edges = []
        while True:
            try:
                f = stories.next()
            except StopIteration:
                break
            for link in f['story_links']:
                if link['inlink']:
                    if f['guid'] not in nodes:
                        nodes.append({'id': f['guid'], 'label': f['guid']})
                    if link['href'] not in nodes:
                        nodes.append({'id': link['href'], 'label': link['href']})
                    edges.append({'id': '%s - %s' % (f['guid'], link['href']), 'source': f['guid'], 'target': link['href']})
        return {'nodes': nodes, 'edges': edges}

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

    def getNeighbors(self, url, spider=0, linksin=True, linksout=True):
        results = []
        urls = [url]
        for i in range(spider+1):
            match_query = {'$or': []}
            if linksout:
                match_query['$or'].extend([
                    {'guid': {'$in': urls}},
                    {'url': {'$in': urls}}
                ])
            if linksin:
                match_query['$or'].append(
                    {'story_links.href': {'$in': urls}}
                )
            # Look for these url as both a source and a target (href)
            response = self._db.stories.aggregate([{'$match': match_query}])['result']
            urls = []
            for item in response:
                spider_level = i
                links = item['story_links']
                inward_links = filter(lambda l: url == l['href'], links)
                if inward_links:
                    # It's a link pointing in
                    spider_level += 1
                    for link in inward_links:
                        link['is_target'] = True
                        link['spider_level'] = spider_level
                        link['href'] = item['guid']
                        results.append(link)
                        links.remove(link)
                for link in links:
                    link['is_target'] = False
                    link['spider_level'] = spider_level
                    results.append(link)
                    urls.append(link['href'])
        return results

    def getStoryLinkData(self, query=None, with_metadata=True):
        if query is None:
            query = {}
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
        y = self.db.getStories(query)
        rows = []
        while True:
            try:
                s = y.next()
            except StopIteration:
                break
            res = {
                '_id': s['_id'],
                'guid': s['guid'],
                'url': s['url'],
                'stories_id': s['stories_id'],
                'media_id': s['media_id'],
                'title': s['title'].encode('ascii', 'ignore'),
                'pubdate': s['publish_date'],
                'num_links': len(s['story_links']),
                'num_inlinks': len(filter(lambda l: l['inlink'] is True, s['story_links'])),
                'hrefs': ', '.join([i['href'] for i in s['story_links']])
            }
            rows.append(res)
        return rows

    def getEdgeRows(self, query):
        rows = self.db.getEdges(query)['edges']
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
