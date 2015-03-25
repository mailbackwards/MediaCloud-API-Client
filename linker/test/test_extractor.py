import unittest
import extractor

class ExtractorTest(unittest.TestCase):

	TEST_URL = 'http://www.theguardian.com/us-news/2015/mar/21/police-killings-us-government-statistics'


	def setUp(self):
		self._extractor = extractor.LinkExtractor(self.TEST_URL)

	def testExtract(self):
		data = self._extractor.extract()
		assert len(data['links']) == 16
		link = data['links'][1]
		for attr in ('para', 'anchor', 'href', 'inlink'):
			assert link.has_key(attr)
