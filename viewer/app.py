from urlparse import urlparse

from os import sys, path
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from linker.querier import CustomStoryDatabase

from flask import Flask, render_template, jsonify, request
app = Flask(__name__)

DB_NAME = 'mclinksspider'

def get_date_from_url(url):
    try:
        datelist = urlparse(url).path.split('/')[1:4]
        for i, date in datelist:
            if len(date) == 1:
                datelist[i] = '0'+date
        return '-'.join(datelist)
    except:
        return ''

def get_title_from_url(url):
    try:
        title = urlparse(url).path.split('/')[-1]
        if title == 'index.html':
            title = urlparse(url).path.split('/')[-2]
        return title
    except:
        return ''

@app.route('/')
def hello_world():
    return render_template('index.html')

@app.route('/hello.json')
def hello_json():
    url = request.args.get('url').strip()
    mode = request.args.get('mode') or 'explore'
    sort_mode = request.args.get('sortMode') or 'date'
    c = CustomStoryDatabase(DB_NAME)

    if mode == 'explore':
        neighbors = c.getNeighbors(url)
    elif mode == 'community':
        spider = int(request.args.get('spider',3))
        neighbors = c.getSpiderCommunity(url, spider=spider)
    elif mode == 'cocitation':
        neighbors = c.getCocitations(url)

    for i, neighbor in enumerate(neighbors):
        neighbors[i].pop('_id', None)
        if neighbors[i].get('href') is None:
            neighbors[i]['href'] = neighbor.get('guid', neighbor.get('url'))
        if neighbors[i].get('title') is None:
            neighbors[i]['title'] = get_title_from_url(neighbor.get('href', neighbor.get('guid')))
        if neighbors[i].get('publish_date') is None:
            neighbors[i]['publish_date'] = get_date_from_url(neighbor.get('href', neighbor.get('guid')))

    if sort_mode == 'date':
        neighbors = sorted(neighbors, key=lambda n: n['publish_date'], reverse=True)
    elif sort_mode == 'size':
        neighbors = sorted(neighbors, key=lambda n: n.get('wordcount',0), reverse=True)
    elif sort_mode == 'relevancy':
        pass

    return jsonify({'results': neighbors})

if __name__ == '__main__' and __package__ is None:
    app.run(debug=True)