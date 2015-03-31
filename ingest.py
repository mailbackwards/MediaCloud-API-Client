import csv
import re
from datetime import date
from linker.ingester import MediaCloudIngester
from linker.querier import CustomStoryDatabase, CsvQuerier

DB_NAME = 'mclinkstest'
START = date(2015,1,1)
END = date(2015,5,1)
#MEDIA_ID = 1751
MEDIA_ID = 104828
MEDIA_SET_ID = None
LAST_ID = 352446818
#LAST_ID = 167885185


def ingest():
    m = MediaCloudIngester(db_name=DB_NAME)
    m.time_series_ingest(START,END,
        media_id=MEDIA_ID,
        media_set_id=MEDIA_SET_ID,
        last_id=LAST_ID)

def _writeToCsv(rows, outfile='out/outfile.csv'):
    if not rows:
        return None
    with open(outfile, 'w+') as f:
        if isinstance(rows[0], dict):
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
        else:
            writer = csv.writer(f)
        for row in rows:
            try:
                writer.writerow(row)
            except UnicodeEncodeError:
                print 'Bad encoding on row %s' % row

def make_pattern_csv(indata):
    """Output overall metadata about a given URL pattern from the database."""
    rows = CsvQuerier(**indata).getPatternData()
    _writeToCsv(rows, 'out/%s_patterns.csv' % indata['label'])

def make_stories_csv(indata):
    """Output a list of stories and their metadata based on the given indata."""
    query = {}
    pattern = '('+'|'.join(indata['patterns'])+')'
    if 'media_id' in indata:
        query['media_id'] = indata['media_id']
    rows = CsvQuerier(db_name=indata['db_name']).getNodeRows(query)
    _writeToCsv(rows, 'out/%s_stories.csv' % indata['label'])

def make_links_csv(indata):
    """Output a network graph spreadsheet with nodes and edges."""
    query = {'media_id': indata['media_id'], 'guid': {'$regex': '('+'|'.join(indata['patterns'])+')'}}
    rows = CsvQuerier(db_name=indata['db_name']).getEdgeRows(query)
    _writeToCsv(rows, 'out/%s_links.csv' % indata['label'])

def removeMatchingStories(patterns):
    c = CustomStoryDatabase('mclinksblogs')
    query = {'$or': [{'guid': {'$in': [re.compile(pattern) for pattern in patterns]}},
                     {'url': {'$in': [re.compile(pattern) for pattern in patterns]}}]}
    c._db.stories.remove(query)

TOP25_DATA = {
    'label': 'top25',
    'db_name': 'mclinksmass',
    #'media_id': 1,
    #'patterns': ('.+',)
    'patterns': ('.+', 'nytimes\.', 'sfgate\.', 'cnet\.', 'nypost\.', 'bostonherald', 
        'cbsnews\.', 'foxnews\.', 'latimes\.', 'nbcnews\.', 'nydailynews\.', 'reuters\.', 
        'guardian\.', 'washingtonpost\.', 'cnn\.', 'usatoday\.', 'telegraph\.', 'bbc\.', 'dailymail\.',
        'examiner\.', 'forbes\.'),
    #'time\(/|\.)',
    'use_guid': False
}

BLOG_DATA = {
    'label': 'blogs',
    'db_name': 'mclinksblogs',
    'patterns': ('.+',)
}

HUFFPO_DATA = {
    'label': 'huffpo',
    'db_name': 'mclinkstest',
    'media_id': 27502,
    'topic_urls': (),
    'patterns': ('.+',)
}

VOX_DATA = {
    'label': 'vox',
    'db_name': 'mclinkstest',
    'media_id': 104828,
    'topic_urls': (),
    'patterns': ()
}

WSJ_DATA = {
    'label': 'wsj',
    'db_name': 'mclinkstest',
    'media_id': 1150,
    'topic_urls': (),
    'patterns': ()
}

NYT_DATA = {
    'label': 'nyt',
    'db_name': 'mclinkstest',
    'media_id': 1,
    'topic_urls': ('topics.nytimes.com', 'movies.nytimes.com', 'dealbook.on.nytimes.com'),
    # 'patterns': ('\/magazine\/',)
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
    'label': 'guardian',
    'db_name': 'mclinkstest',
    'media_id': 1751,
    'topic_urls': (),
    #'patterns': ('\/artanddesign\/',)
    'patterns': ('.+',
                '\/artanddesign\/',
                '\/books\/',
                '\/business\/',
                '\/childrens-books-site\/',
                '\/cities\/',
                '\/commentisfree\/',
                '\/crosswords\/',
                '\/culture\/',
                '\/culture-professionals-network\/',
                '\/education\/',
                '\/environment\/',
                '\/extra\/',
                '\/fashion\/',
                '\/film\/',
                '\/football\/',
                '\/global\/',
                '\/global-development\/',
                '\/global-development-professionals-network\/',
                '\/healthcare-network\/',
                '\/higher-education-network\/',
                '\/housing-network\/',
                '\/info\/',
                '\/law\/',
                '\/lifeandstyle\/',
                '\/local-government-network\/',
                '\/media\/',
                '\/media-network\/',
                '\/money\/',
                '\/music\/',
                '\/news\/',
                '\/owntheweekend\/',
                '\/politics\/',
                '\/public-leaders-network\/',
                '\/science\/',
                '\/small-business-network\/',
                '\/social-care-network\/',
                '\/social-enterprise-network\/',
                '\/society\/',
                '\/sport\/',
                '\/stage\/',
                '\/student-on-a-budget-partner-zone\/',
                '\/sustainable-business\/',
                '\/teacher-network\/',
                '\/technology\/',
                '\/theguardian\/',
                '\/theobserver\/',
                '\/travel\/',
                '\/tv-and-radio\/',
                '\/uk-news\/',
                '\/voluntary-sector-network\/',
                '\/weather\/',
                '\/what-is-nano\/',
                '\/women-in-leadership\/',
                '\/world\/',
        )
}

if __name__ == "__main__":
    ingest()
