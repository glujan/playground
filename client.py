import argparse
import asyncio as aio
import logging
import random
import sys
from signal import SIGTERM, SIGINT

from aiohttp import ClientSession
from asyncio_redis import Pool as RedisPool, Connection as RedisConn


REDIS_CONF = {
    'host': 'localhost',
    'port': 6379,
    'poolsize': 100,
}
LOGGER = 'playground'


class FeedUpdater(object):

    def __init__(self, loop):
        self._redis = None

    async def _fetch(self, session: ClientSession):
        logger = logging.getLogger(LOGGER)
        while True:
            url = (await self._redis.blpop(['urls', ])).value
            async with session.get(url) as response:
                response = await response.read()
                logger.info('Fetched: %s', url)

    async def run(self):
        logger = logging.getLogger(LOGGER)
        self._redis = await RedisPool.create(**REDIS_CONF)
        async with ClientSession() as session:
            tasks_count = REDIS_CONF['poolsize']
            logger.info('Starting to fetch URLs')
            await aio.wait(
                tuple(self._fetch(session) for i in range(tasks_count))
            )
            self._redis.close()


async def _populate_queue():
    url = "http://localhost:8080/{}"
    i = 0
    conn = await RedisConn.create(host=REDIS_CONF['host'], port=REDIS_CONF['port'])
    while True:
        await conn.lpush('urls', [url.format(i), ])
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


def _setup_logging(loglevel):
    LEVELS = {
        0: logging.WARNING,
        1: logging.INFO,
        2: logging.DEBUG,
        3: logging.NOTSET,
    }

    logging.basicConfig(level=LEVELS.get(loglevel, 3))


def _setup_event_loop(debug=False):
    def ask_exit():
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

    logging.getLogger(LOGGER).debug('Starting event loop...')
    loop.run_forever()
