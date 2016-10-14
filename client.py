import argparse
import asyncio as aio
import logging
import random
import sys
from signal import SIGTERM, SIGINT

from aiohttp import ClientSession
from asyncio_redis import Pool as RedisPool, Connection as RedisConn
from asyncio_redis.encoders import BytesEncoder
from feedparser import parse as parse_feed
from google.protobuf.message import Message

from utils import create_entry, create_feed


REDIS_CONF = {
    'host': 'localhost',
    'port': 6379,
    'poolsize': 100,
    'encoder': BytesEncoder()
}

R_URLS = b'urls'
R_FEED = b'feed'
LOGGER = 'playground'


class FeedError(ValueError):
    "Raise when cannot parse RSS/Atom feed"


class FeedUpdater(object):

    def __init__(self, loop: aio.AbstractEventLoop) -> None:
        self._loop = loop
        self._redis = None  # type: RedisPool

    def parse(self, xml: str) -> Message:
        parsed = parse_feed(xml)
        if parsed.bozo:
            raise FeedError

        feed = create_feed(parsed.feed)
        entries = list(map(create_entry, parsed.entries))
        # TODO Filter out entries older than last visit time
        feed.entries.extend(entries)

        return feed

    async def execute(self, url: str, session: ClientSession):
        response = None  # type: str
        logger = logging.getLogger(LOGGER)
        async with session.get(url) as response:  # FIXME Handle aiohttp.errors.ClientOSError
            response = await response.read()

        try:
            feed = await self._loop.run_in_executor(None, self.parse, response)
        except FeedError:
            logger.warn('Invalid feed: %s', url)
            # TODO Handle FeedError properly
        else:
            logger.info('Parsed feed: %s', url)
            await self._redis.lpush(R_FEED, [feed.SerializePartialToString(), ])

    async def run(self):
        logger = logging.getLogger(LOGGER)
        self._redis = await RedisPool.create(**REDIS_CONF)
        session = ClientSession()
        logger.info('Starting up FeedUpdater')
        try:
            while True:
                url = (await self._redis.blpop([R_URLS, ])).value.decode('utf8')
                self._loop.create_task(self.execute(url, session))
        finally:
            self._redis.close()
            session.close()


async def _populate_queue():
    url = "http://localhost:8080/{}"
    i = 0
    redis_conf = {
        'host': REDIS_CONF.get('host'),
        'port': REDIS_CONF.get('port'),
        'encoder': REDIS_CONF.get('encoder'),
    }
    conn = await RedisConn.create(**redis_conf)
    while True:
        await conn.lpush(R_URLS, [bytes(url.format(i), 'utf8'), ])
        await aio.sleep(random.random())
        i += 1


def _setup_argparse():
    parser = argparse.ArgumentParser(description='Fetch RSS/Atom feeds asynchronously.')
    parser.add_argument("-p", "--populate", action="store_true",
                        help='Populate Redis with fake URLs')
    parser.add_argument("-v", "--verbosity", action="count", default=0,
                        help='Increase output verbosity')
    parser.add_argument("--debug", action="store_true",
                        help='Set event loop to DEBUG mode')

    return parser.parse_args()


def _setup_logging(loglevel: int):
    LEVELS = {
        0: logging.WARNING,
        1: logging.INFO,
        2: logging.DEBUG,
        3: logging.NOTSET,
    }

    logging.basicConfig(level=LEVELS.get(loglevel, 3))


def _setup_event_loop(debug=False):
    def ask_exit():
        logger = logging.getLogger(LOGGER)
        if logger.getEffectiveLevel() >= logging.WARN:
            logger.warn('Killing %d tasks', len(aio.Task.all_tasks()))
        logger.info('Stopping an event loop')
        loop.stop()
        logging.shutdown()
        sys.exit(0)

    loop = aio.get_event_loop()
    loop.set_debug(debug)
    if not debug:
        loop.add_signal_handler(SIGINT, ask_exit)
        loop.add_signal_handler(SIGTERM, ask_exit)

    return loop


if __name__ == '__main__':
    args = _setup_argparse()
    loop = _setup_event_loop(args.debug)
    _setup_logging(args.verbosity)

    if args.populate:
        aio.ensure_future(_populate_queue())

    updater = FeedUpdater(loop)
    aio.ensure_future(updater.run())

    logging.getLogger(LOGGER).debug('Starting an event loop')
    loop.run_forever()
