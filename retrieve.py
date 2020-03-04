#!/usr/bin/env python3

from argparse import ArgumentParser
from logging import getLogger
from lxml import etree
from pathlib import Path
from pprint import pprint
import requests
from time import sleep
from urllib.parse import urljoin
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, ForeignKey, UniqueConstraint


logger = getLogger(__name__)

here = Path(__file__).resolve().parent

cache_dir = here / 'cache'

rs = requests.session()

metadata = MetaData()

table_movies = Table('movies', metadata,
    Column('id', Integer, primary_key=True),
    Column('title', String),
    Column('csfd_url', String, unique=True),
    Column('year', Integer))

table_actors = Table('actors', metadata,
    Column('id', Integer, primary_key=True),
    Column('csfd_url', String, unique=True),
    Column('name', String))

table_movie_to_actor = Table('movie_to_actor', metadata,
    Column('id', Integer, primary_key=True),
    Column('movie_id', Integer, nullable=False),
    Column('actor_id', Integer, nullable=False),
    UniqueConstraint('movie_id', 'actor_id'))


def main():
    p = ArgumentParser()
    p.add_argument('--db', default='sqlite:///movies.sqlite')
    args = p.parse_args()
    setup_logging()
    if not cache_dir.exists():
        cache_dir.mkdir()
    engine = create_engine(args.db)
    metadata.create_all(engine)
    get_top_movies(engine)


def get_top_movies(engine):
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
        movie_title = a.text
        logger.debug('movie_title: %r', movie_title)
        movie_url = urljoin(top_url, a.attrib['href'])
        logger.debug('movie_url: %r', movie_url)

        res = engine.execute('SELECT id FROM movies WHERE csfd_url = :url', url=movie_url)
        row = res.first()
        if row:
            movie_id = row['id']
        else:
            sql = 'INSERT INTO movies (title, csfd_url, year) VALUES (:title, :csfd_url, :year)'
            res = engine.execute(sql, title=movie_title, csfd_url=movie_url, year=year)
            movie_id = res.lastrowid

        get_movie(engine, movie_id, movie_url)


def get_movie(engine, movie_id, movie_url):
    html = retrieve(movie_url)
    root = etree.HTML(html)
    h4, = root.xpath('//div/h4[text()="Hrají:"]')
    for a in h4.getparent().xpath('.//a'):
        logger.debug('a: %s', etree.tostring(a))
        actor_name = a.text
        actor_url = urljoin(movie_url, a.attrib['href'])
        logger.debug('actor_name: %r', actor_name)
        logger.debug('actor_url: %r', actor_url)

        res = engine.execute('SELECT id FROM actors WHERE csfd_url = :url', url=actor_url)
        row = res.first()
        if row:
            actor_id = row['id']
            new_actor = False
        else:
            sql = 'INSERT INTO actors (name, csfd_url) VALUES (:name, :csfd_url)'
            res = engine.execute(sql, name=actor_name, csfd_url=movie_url)
            actor_id = res.lastrowid
            new_actor = True

        sql = '''
            INSERT INTO movie_to_actor (movie_id, actor_id)
            VALUES (:movie_id, :actor_id)
        '''
        engine.execute(sql, movie_id=movie_id, actor_id=actor_id)

        assert 0

        if new_actor:
            get_actor(engine, actor_id, actor_url)


def get_actor(engine, actor_id, actor_url):
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



