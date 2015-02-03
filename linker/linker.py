import logging
import os
import sys
import re
import csv
from datetime import date
from mediacloud.api import CustomMediaCloud
from mediacloud.storage import CustomStoryDatabase
from mediacloud.error import CustomMCException
from .extractor import LinkExtractor

loghandler = logging.StreamHandler(stream=sys.stdout)
def setHandling(logger):
	logger.addHandler(loghandler)
	logger.setLevel(logging.DEBUG)
	return logger

DB_NAME = 'mclinks'
API_KEY = os.environ.get('MEDIA_CLOUD_API_KEY') or None

DEFAULT_SOLR_QUERY = '*'
DEFAULT_SOLR_FILTER = 'media_id:1' # Just NYT stories

class MCLinkerException(CustomMCException):
	def __init__(self, message, status_code=0, mc_resp=None):
		CustomMCException.__init__(self, message, status_code, mc_resp)


class MediaCloudLinker:
	"""
	Bringing together the API, the database, and the link extractor.
	"""

	def __init__(self, api_key=None, db_name=None):
		db = db_name or DB_NAME
		key = api_key or API_KEY
		db = CustomStoryDatabase(db)
		db._logger = setHandling(db._logger)
		self.db = db
		api = CustomMediaCloud(key)
		api._logger = setHandling(api._logger)
		self.api = api
		self._logger = setHandling(logging.getLogger(__name__))

	def getStories(self, solr_query='', solr_filter='', last_processed_stories_id=0, rows=20, raw_1st_download=True):
		return self.api.storyList(
			solr_query=solr_query,
			solr_filter=solr_filter,
			last_processed_stories_id=last_processed_stories_id,
			rows=rows,
			raw_1st_download=raw_1st_download
			)

	def processStory(self, story):
		"""
		Extracts links from a story and saves it to the database.
		"""
		links = LinkExtractor(story['url'], 
			html=story.pop('raw_first_download_file'), 
			source_url=story['media_url'])\
			.extract()
		self.db.addStory(story, extra_attributes={'story_links': links})
		return story, links


	def ingest(self, solr_query='', solr_filter='', last_processed_stories_id=0, rows=20):
		"""
		Queries MediaCloud for a set of stories, extracts its links and saves the results to the database.
		"""
		self._logger.debug('Querying with last_id of %s' % last_processed_stories_id)
		try:
			stories = self.getStories(solr_query=solr_query, solr_filter=solr_filter, last_processed_stories_id=last_processed_stories_id, rows=rows)
		except CustomMCException as e:
			mc_err_msg = e.mc_resp.json()['error']
			self._logger.warn('Failed with message %s' % mc_err_msg)
			if 'unsuccessful download' in mc_err_msg:
				bad_id = re.search(r'unsuccessful download ([0-9]+)', mc_err_msg).groups()[0]
				# this is the download id so we need to query the API for the story id
				try:
					bad_story_id = self.api.download(bad_id)['stories_id']
				except CustomMCException as err:
					raise MCLinkerException(err.mc_resp.json()['error'], err.status_code, err.mc_resp)
				solr_filter = solr_filter[:-1] + ' OR ' + str(bad_story_id) + solr_filter[-1]
				self._logger.warn('Bad download id of %s, changing solr_filter' % bad_id)
				return self.ingest(solr_query=solr_query, solr_filter=solr_filter, last_processed_stories_id=last_processed_stories_id, rows=rows)
			else:
				raise MCLinkerException(mc_err_msg, e.status_code, e.mc_resp)
		for story in stories:
			try:
				story, links = self.processStory(story)
				self._logger.debug('Found story processed_id %s with %d links' % (story['processed_stories_id'], links['num_links']))
			except:
				self._logger.warn('FAILED on story processed_id %s guid %s' % (story['processed_stories_id'], story['guid']))
		if not stories:
			self._logger.debug('No stories')
		return stories

	def ingest_all(self, solr_query=None, solr_filter=None, last_id=0):
		"""
		Continuously queries MediaCloud for all stories matching the given query until it's done.
		Extracts links from each story and saves it to the database.
		"""
		rows = 100
		solr_query = solr_query or DEFAULT_SOLR_QUERY
		solr_filter = solr_filter or DEFAULT_SOLR_FILTER
		self._logger.debug('Starting mass ingestion with query %s and filter %s' % (solr_query, solr_filter))
		while True:
			try:
				stories = self.ingest(solr_query, solr_filter, last_id, rows)
			except CustomMCException as e:
				self._logger.error('Failed with message %s' % e.message)
				break
			if not stories:
				self._logger.debug('No more stories. Done ingesting.')
				break
			last_id = stories[-1]['processed_stories_id']

	def media_time_series_ingest(self, start_date, end_date, media_id, last_id=0):
		start_datestr = start_date.strftime('%Y-%m-%d')
		end_datestr = end_date.strftime('%Y-%m-%d')
		solr_filter = '+publish_date:[{start}T00:00:00Z TO {end}T23:59:59Z] AND media_id:{media}'.format(
			start=start_date,
			end=end_date,
			media=media_id)
		self.ingest_all(solr_filter=solr_filter, last_id=last_id)


class CsvQuerier(object):

    def __init__(self, outfile='outfile.csv', db_name='mclinks', media_id=None, topic_urls=None, patterns=None):
        self.outfile = outfile
        self.db = CustomStoryDatabase(db_name)
        self.media_id = media_id
        self.topic_urls = topic_urls or ()
        self.patterns = patterns or ()

    def _writeDictToCsv(self, rows, outfile=None):
    	if outfile is None:
    		outfile = self.outfile
        if not rows:
            return None
        with open(outfile, 'w+') as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            for row in rows:
                writer.writerow(row)

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
                'title': s['title'].encode('ascii', 'ignore'),
                'pubdate': s['publish_date'],
                'num_links': s['story_links']['num_links'],
                'num_inlinks': s['story_links']['num_inlinks'],
                'hrefs': ', '.join([i['href'] for i in s['story_links']['links']])
            }
            rows.append(res)
        return rows

    def getEdgeRows(self, query):
        rows = self.db.getEdges(query)['edges']
        return rows

    def runPatterns(self):
    	rows = self.getPatternData()
    	self._writeDictToCsv(rows)

    def getPatternData(self):
        rows = []
        for pattern in self.patterns:
            res = self.db.getLinkData({'media_id': self.media_id, 'guid': {'$regex': pattern}})
            
            res = self.addCustomData(res)

            res.pop('stories')
            res['pattern'] = pattern
            rows.append(res)
        return rows

    def addCustomData(self, res):
	    hrefs = [a['hrefs'][0] for a in res['stories']]
	    total_count = 0
	    topics_count = 0
	    for href in hrefs:
	        for url in href:
	            total_count += 1
	            if any(i in url for i in self.topic_urls):
	                topics_count += 1
	    res['numTopicLinks'] = topics_count
	    res['pctTopicLinks'] = float(topics_count) / total_count
	    return res


def run_nyt_patterns(indata):
	res = CsvQuerier(**indata).runPatterns()

NYT_DATA = {
	'outfile': 'outfile_nyt.csv',
	'db_name': 'mclinks',
	'media_id': 1,
	'topic_urls': ('topics.nytimes.com', 'movies.nytimes.com', 'dealbook.on.nytimes.com'),
	'patterns': (
		'.+', '\.blogs\.nytimes', 'www\.nytimes\.com', 'learning\.blogs\.nytimes', 'dealbook\.nytimes', '\/movies\/', '\/arts\/', '\/music\/', '\/theater\/', 
	    'cityroom\.blogs\.nytimes', '\/books\/', '\/nyregion\/', 'publiceditor\.blogs\.nytimes', '\/technology\/', '\/business\/',
	    '\/sports\/', '\/fashion\/', '\/magazine\/', '\/dining\/', '\/health\/', 'carpetbagger\.blogs\.nytimes', 
	    'bits\.blogs\.nytimes', 'opinionator\.blogs\.nytimes', '\/opinion\/', 'parenting\.blogs\.nytimes', 'artsbeat\.blogs\.nytimes',
	    'thelede\.blogs\.nytimes', 'dotearth\.blogs\.nytimes', 'runway\.blogs\.nytimes', 'thecaucus\.blogs\.nytimes', '\/realestate\/',
	    '\/politics\/', 'economix\.blogs\.nytimes', 'well\.blogs\.nytimes', '\/us\/', '\/world\/', '\/science\/', '\/automobiles\/'
	)
}

GUARDIAN_DATA = {
	'outfile': 'outfile_guardian.csv',
	'db_name': 'mclinks',
	'media_id': 1751,
	'topic_urls': (),
	'patterns': ()
}
