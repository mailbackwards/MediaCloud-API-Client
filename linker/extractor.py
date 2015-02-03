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

def is_inlink(target_url, src_urls):
	"""Checks the target_url domain against all possible src_urls, and returns true if there are any domain matches."""
	if not isinstance(src_urls, list):
		src_urls = [src_urls]
	if target_url.startswith('http') or target_url.startswith('//'):
		target_url_domain = get_domain(target_url)
		source_url_domains = map(lambda u: get_domain(u), src_urls)
		if not any(target_url_domain == d for d in source_url_domains):
			return False
	return True

def get_domain(url):
	return tldextract.extract(url).domain

class LinkExtractor:
	"""
	Extract metadata about all the links in a news article.
	"""

	def __init__(self, url, html=None, source_url=u''):
		article = Article(url, language='en', keep_article_html=True)
		article.download(html=html)
		article.parse()
		self.extractor = article
		self.source_url = source_url

	def strip_args(self, url):
	    """ Accepts URL as a string and strips arguments, avoiding flags """
	    FLAGS = ['on.nytimes.com/public/overview', 'query.nytimes.com']
	    
	    if not any(flag in url.lower() for flag in FLAGS):
	        for i in range(len(url)):
	            if url[i] == "?" or url[i] == "#":
	                url_str = url[:i]
	                if url_str.endswith('/all/'):
	                    url_str = url_str[:-4]
	                return url_str
	    return url

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
				href = self.strip_args(a['href'])
				links.append({
					'href': href,
					'anchor': a.get_text(),
					'inlink': is_inlink(href, [self.extractor.url, self.extractor.canonical_link, self.source_url]),
					'para': '%s/%s' % (i+1, len(all_nodes)),
					'_raw_attrs': a.attrs
				})
		data = {
			'num_links': len(links),
			'num_inlinks': len(filter(lambda l: l['inlink'] == True, links)),
			'links': links
		}
		return data
