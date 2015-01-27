from urlparse import urlparse
import tldextract
from newspaper import Article
from bs4 import BeautifulSoup

DEFAULT_CONTENT_NODE_TYPES = ['p']

def content_nodes(elem, node_types=None):
	if node_types is None:
		node_types = DEFAULT_CONTENT_NODE_TYPES
	return elem.find_all(node_types)

def is_valid_weblink(attr):
	return attr and not attr.startswith('mailto:')

def get_domain(url):
	return tldextract.extract(url).domain

class LinkExtractor:

	def __init__(self, url, html=None, source_url=u''):
		article = Article(url, language='en', keep_article_html=True)
		article.download(html=html)
		article.parse()
		self.extractor = article
		self.source_url = source_url

	def article_soup(self):
		soup = BeautifulSoup(self.extractor.article_html)
		return soup

	def extract(self):
		links = []
		article_soup = self.article_soup()
		all_nodes = content_nodes(article_soup)
		if not all_nodes:
			all_nodes = [article_soup]
		for i, n in enumerate(all_nodes):
			for a in n.find_all('a', href=is_valid_weblink):
				links.append({
					'href': a['href'],
					'anchor': a.get_text(),
					'inlink': self.is_inlink(a['href']),
					'para': '%s/%s' % (i+1, len(all_nodes)),
					'_raw_attrs': a.attrs
				})
		
		data = {
			'num_links': len(links),
			'num_inlinks': len(filter(lambda l: l['inlink'] == True, links)),
			'links': links
		}
		return data

	def is_inlink(self, url):
		if url.startswith('http') or url.startswith('//'):
			target_url_domain = get_domain(url)
			# Try the article urls and source url
			source_urls = [self.extractor.url, self.extractor.canonical_link, self.source_url]
			source_url_domains = map(lambda u: get_domain(u), source_urls)
			if not any(target_url_domain == d for d in source_url_domains):
				return False
		return True
