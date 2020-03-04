#!/usr/bin/env python3

from argparse import ArgumentParser
from logging import getLogger
from lxml import etree
from pathlib import Path
from pprint import pprint
import requests
from time import sleep
from urllib.parse import urljoin


logger = getLogger(__name__)

here = Path(__file__).resolve().parent

cache_dir = here / 'cache'

rs = requests.session()


def main():
    p = ArgumentParser()
    args = p.parse_args()
    setup_logging()
    if not cache_dir.exists():
        cache_dir.mkdir()
    movies = {}
    get_top_movies(movies)
    pprint(movies, width=120)


def get_top_movies(movies):
    top_url = 'https://www.csfd.cz/zebricky/nejlepsi-filmy/?show=complete'
    html = retrieve(top_url)
    root = etree.HTML(html)
    table, = root.xpath('//div[@id="results"]/table[@class="content ui-table-list striped"]')
    for n, tr in enumerate(table.xpath('./tr'), start=1):
        logger.debug('tr %d: %s', n, etree.tostring(tr))
        if b'caroda-slot' in etree.tostring(tr) or list(tr) == []:
            continue
        span_year, = tr.xpath('./td/span[@class="film-year"]')
        year = int(span_year.text.strip('()'))
        a, = tr.xpath('./td[@class="film"]/a')
        title = a.text
        logger.debug('title: %r', title)
        link = urljoin(top_url, a.attrib['href'])
        logger.debug('link: %r', link)
        movies.setdefault(link, {})
        movies[link]['csfd_url'] = link
        movies[link]['title'] = title
        movies[link]['year'] = year
        get_movie(movies, link)


def get_movie(movies, movie_url):
    html = retrieve(movie_url)
    root = etree.HTML(html)
    h4, = root.xpath('//div/h4[text()="Hrají:"]')
    for a in h4.getparent().xpath('.//a'):
        logger.debug('a: %s', etree.tostring(a))
        name = a.text
        link = urljoin(movie_url, a.attrib['href'])
        logger.debug('name: %r link: %r', name, link)
        get_actor(link)


def get_actor(actor_url):
    html = retrieve(actor_url)



def retrieve(url):
    cache_filename = url.replace('/', '_').replace(':', '')
    cache_path = cache_dir / cache_filename
    if not cache_path.is_file():
        headers = {
            'User-Agent': 'Mozilla/4.0',
        }
        r = rs.get(url, headers=headers, timeout=30)
        logger.debug('Response: %r', r.text[:200])
        r.raise_for_status()
        cache_path.write_text(r.text)
        sleep(.5)
    else:
        logger.debug('Cached: %s', url)
    return cache_path.read_text()


def setup_logging():
    from logging import basicConfig, DEBUG
    basicConfig(
        format='%(asctime)s %(name)s %(levelname)5s: %(message)s',
        level=DEBUG)


if __name__ == '__main__':
    main()



