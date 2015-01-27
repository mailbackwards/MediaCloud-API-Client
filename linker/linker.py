import logging
import os
import sys
import re
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
DEFAULT_SOLR_FILTER = '+publish_date:[2014-01-01T00:00:00Z TO 2014-01-01T23:59:59Z] AND media_id:3'

class MCLinkerException(CustomMCException):
	def __init__(self, message, status_code=0, mc_resp=None):
		CustomMCException.__init__(self, message, status_code, mc_resp)

class MediaCloudLinker:

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

	def getDownloadInfo(self, download_id):
		return self.api.download(download_id)

	def getStories(self, solr_query='', solr_filter='', last_processed_stories_id=0, rows=20):
		stories = self.api.storyList(
			solr_query=solr_query,
			solr_filter=solr_filter,
			last_processed_stories_id=last_processed_stories_id,
			rows=rows,
			raw_1st_download=True
			)
		return stories

	def getLinks(self, story):
		return LinkExtractor(story['url'], html=story.pop('raw_first_download_file'), source_url=story['media_url']).extract()

	def saveLinkedStory(self, story, links):
		self.db.addStory(story, extra_attributes={'story_links': links})

	def getLinkData(self, query=None, with_metadata=True):
		if query is None:
			query = {}
		stories = self.db.getLinkData(query)['result']
		data = {
			'numStories': len(stories),
			'stories': stories
		}
		if with_metadata:
			linkdata = [s['num_links'] for s in stories]
			inlinkdata = [s['num_inlinks'] for s in stories]

			num_with_links = len(filter(lambda s: s['num_links']>0, stories))
			num_with_inlinks = len(filter(lambda s: s['num_inlinks']>0, stories))
			
			data.update({
				'avgLinks': float(sum(linkdata)) / len(linkdata),
				'avgInlinks': float(sum(inlinkdata)) / len(inlinkdata),
				'maxLinks': max(linkdata),
				'numWithLinks': num_with_links,
				'numWithInlinks': num_with_inlinks,
				'pctWithLinks': '%.2f' % (float(num_with_links) / len(stories) * 100),
				'pctWithInlinks': '%.2f' % (float(num_with_inlinks) / len(stories) * 100)
			})
		return data

	def process(self, solr_query='', solr_filter='', last_processed_stories_id=0, rows=20):
		self._logger.debug('Querying with last_id of %s' % last_processed_stories_id)
		try:
			stories = self.getStories(solr_query=solr_query, solr_filter=solr_filter, last_processed_stories_id=last_processed_stories_id, rows=rows)
		except CustomMCException as e:
			mc_err_msg = e.mc_resp.json()['error']
			self._logger.warn('Failed with message %s' % mc_err_msg)
			if 'unsuccessful download' in mc_err_msg:
				bad_id = re.search(r'unsuccessful download ([0-9]+)', mc_err_msg).groups()[0]
				# this is the download id so we need to query the API for the story id
				bad_story_id = mc.getDownloadInfo(bad_id)['stories_id']
				solr_filter = solr_filter[:-1] + ' OR ' + str(bad_story_id) + solr_filter[-1]
				self._logger.warn('Bad download id of %s, changing solr_filter' % bad_id)
				return self.process(solr_query=solr_query, solr_filter=solr_filter, last_processed_stories_id=last_processed_stories_id, rows=rows)
			else:
				raise MCLinkerException(mc_err_msg, e.status_code, e.mc_resp)
		for story in stories:
			try:
				links = self.getLinks(story)
				self._logger.debug('Found story processed_id %s with %d links' % (story['processed_stories_id'], links['num_links']))
				self.saveLinkedStory(story, links)
			except:
				self._logger.warn('FAILED on story processed_id %s guid %s' % (story['processed_stories_id'], story['guid']))
		if not stories:
			self._logger.debug('No more stories')
		return stories

	def process_multi(self, solr_query=None, solr_filter=None):
		last_id = 0
		rows = 100
		solr_query = solr_query or DEFAULT_SOLR_QUERY
		solr_filter = solr_filter or DEFAULT_SOLR_FILTER
		self._logger.debug('Starting multiprocess with query %s and filter %s' % (solr_query, solr_filter))
		while True:
			try:
				stories = self.process(solr_query, solr_filter, last_id, rows)
			except CustomMCException as e:
				self._logger.error('Failed with message %s' % e.message)
				break
			if not stories:
				self._logger.debug('No more stories. Done processing query.')
				break
			last_id = stories[-1]['processed_stories_id']

def example():
	mc = MediaCloudLinker()
	mc.process_multi()
