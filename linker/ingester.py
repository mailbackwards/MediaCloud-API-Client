import logging
import os
import sys
import re
import hashlib
from mediacloud.api import CustomMediaCloud
from mediacloud.error import CustomMCException
from .extractor import LinkExtractor
from .querier import CustomStoryDatabase

loghandler = logging.StreamHandler(stream=sys.stdout)
def setHandling(logger):
	logger.addHandler(loghandler)
	logger.setLevel(logging.DEBUG)
	return logger

DB_NAME = 'mclinks'
API_KEY = os.environ.get('MEDIA_CLOUD_API_KEY') or None

class MCLinkerException(CustomMCException):
	def __init__(self, message, status_code=0, mc_resp=None):
		CustomMCException.__init__(self, message, status_code, mc_resp)


class MediaCloudIngester:
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
		data = LinkExtractor(story['url'], 
			html=story.pop('raw_first_download_file'), 
			source_url=story['media_url'])\
			.extract()
		self.db.addStory(story, extra_attributes=data)
		return story, data


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
				story, data = self.processStory(story)
				self._logger.debug('Found story processed_id %s with %d links' % (story['processed_stories_id'], len(data['story_links'])))
			except:
				self._logger.warn('Failed on story processed_id %s guid %s' % (story['processed_stories_id'], story['guid']))
		if not stories:
			self._logger.debug('No stories')
		return stories

	def ingest_all(self, solr_query=None, solr_filter=None, last_id=0):
		"""
		Continuously queries MediaCloud for all stories matching the given query until it's done.
		Extracts links from each story and saves it to the database.
		"""
		rows = 100
		solr_query = solr_query or '*'
		solr_filter = solr_filter or '*'
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

	def time_series_ingest(self, start_date, end_date, media_id=None, media_set_id=None, last_id=0):
		"""
		Continuously queries MediaCloud for all stories matching the given
		start_date, end_date, media_id and media_set_id.
		"""
		start_datestr = start_date.strftime('%Y-%m-%d')
		end_datestr = end_date.strftime('%Y-%m-%d')
		solr_filter = '+publish_date:[{start}T00:00:00Z TO {end}T23:59:59Z]'.format(
			start=start_date, end=end_date)
		if media_id is not None:
			solr_filter += ' AND +media_id:{media}'.format(media=media_id)
		if media_set_id is not None:
			solr_filter += ' AND +media_sets_id:{media_set}'.format(media_set=media_set_id)
		self.ingest_all(solr_filter=solr_filter, last_id=last_id)

class LinkSpider(object):

	def __init__(self, db_name=None):
		db_name = db_name or DB_NAME
		self.db = CustomStoryDatabase(db_name)
		self.hrefs = set()
		self.queue = set()

	def spider_from(self, url, limit=20000):
		print '%d links crawled, %d links in queue. url %s' % (len(self.hrefs), len(self.queue), url)
		if url not in self.hrefs:
			self.hrefs |= set([url])
			data = LinkExtractor(url).extract(get_meta=True)
			data.update({
				'guid': url,
				'stories_id': hashlib.md5(url).hexdigest()
			})
			story = self.db.addStory(data)
			limit -= 1
			# Add the inlinks to a queue so we get the closest links first
			self.queue |= set([link['href'] for link in data['story_links']
							  if link['inlink'] is True and link['href'] not in self.hrefs and not any((subl in link['href'] for subl in ('topics.nytimes', 'movies.nytimes')))])
		else:
			print '--> Already crawled'
		if self.queue:
			return self.spider_from(self.queue.pop(), limit=limit)
		return len(self.hrefs)
