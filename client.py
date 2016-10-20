import argparse
import asyncio as aio
import logging
import random
import sys
import typing
from abc import abstractmethod
from signal import SIGTERM, SIGINT

from aiohttp import ClientSession
from asyncio_redis import Pool as RedisPool
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

R_SRC = b'urls'
R_DST = b'feed'
logger = logging.getLogger('playground')


ConnConf = typing.Dict[str, typing.Any]
Url = typing.NewType('Url', bytes)


class RedisConn:
    def __init__(self, conf: ConnConf) -> None:
        self._conf = conf
        self._conn = None  # type: typing.Any

    async def __aenter__(self) -> 'RedisConn':
        self._conn = await (RedisPool.create(**self._conf))
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        if exc_type is not None:
            self._conn.close()
        else:
            await self._conn.close()

    async def push(self, name: bytes, element: bytes) -> None:
        return await self._conn.lpush(name, [element, ])

    async def pop(self, name: bytes) -> bytes:
        return (await self._conn.blpop([name, ])).value


class BaseUrls(typing.AsyncIterator[Url]):
    def __init__(self, conn: RedisConn, pop_name: bytes) -> None:
        self._conn = conn
        self._pop = pop_name

    def __aiter__(self) -> typing.AsyncIterator[Url]:
        return self

    @abstractmethod
    async def __anext__(self) -> Url:
        pass


class RedisUrls(BaseUrls):
    async def __anext__(self) -> Url:
        data = await self._conn.pop(self._pop)
        return Url(data)


class FakeUrls(BaseUrls):
    def __init__(self, conn: RedisConn, pop_name: bytes) -> None:
        super().__init__(conn, pop_name)
        self._base_url = "http://localhost:8080/{}"
        self._current = 0

    async def __anext__(self) -> Url:
        url = Url(bytes(self._base_url.format(self._current), 'utf8'))
        self._current += 1
        return url


class ParseError(ValueError):
    "Raise when cannot parse data from your source"


class FeedUpdater:
    def __init__(self, loop: aio.AbstractEventLoop, conn: RedisConn) -> None:
        self._loop = loop
        self._conn = conn

    async def execute(self, url: str, session: ClientSession) -> Message:
        try:
            data = await self._get_data(session, url)
            message = await self._loop.run_in_executor(None, self.parse, data)
        except ParseError as e:
            logger.warn('Invalid data source: %s', url)
            raise
        except Exception:
            pass
        else:
            logger.debug('Parsed feed: %s', url)
            await self._conn.push(R_DST, message.SerializePartialToString())
            logger.info('Published feed: %s to %s', url, R_DST.decode('utf8'))
            return message

    async def _get_data(self, session, url: str) -> str:
        async with session.get(url) as raw_data:
            return await raw_data.read()

    def parse(self, data: str) -> Message:
        parsed = parse_feed(data)
        if parsed.bozo:
            raise ParseError

        feed = create_feed(parsed.feed)
        entries = list(map(create_entry, parsed.entries))
        # TODO Filter out entries older than last visit time
        feed.entries.extend(entries)

        return feed


async def main(loop: aio.AbstractEventLoop) -> None:
    async with RedisConn(REDIS_CONF) as conn, ClientSession() as session:
        updater = FeedUpdater(loop, conn)
        async for url in RedisUrls(conn, R_SRC):
            logger.debug("Processing url '%s'", url)
            loop.create_task(updater.execute(url.decode('utf8'), session))
            if len(aio.Task.all_tasks()) > 1000:
                await aio.sleep(1)


async def _populate_queue() -> None:
    local_conf = REDIS_CONF.copy()
    local_conf['poolsize'] = 1

    async with RedisConn(REDIS_CONF) as conn:
        async for url in FakeUrls(conn, R_SRC):
            await conn.push(R_SRC, url)
            await aio.sleep(random.random())


def _setup_argparse() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Fetch RSS/Atom feeds asynchronously.')
    parser.add_argument("-p", "--populate", action="store_true",
                        help='Populate Redis with fake URLs')
    parser.add_argument("-v", "--verbosity", action="count", default=0,
                        help='Increase output verbosity')
    parser.add_argument("--debug", action="store_true",
                        help='Set event loop to DEBUG mode')

    return parser.parse_args()


def _setup_logging(loglevel: int) -> None:
    LEVELS = {
        0: logging.WARNING,
        1: logging.INFO,
        2: logging.DEBUG,
        3: logging.NOTSET,
    }

    logging.basicConfig(level=LEVELS.get(loglevel, 3))


def _setup_event_loop(debug: bool=False) -> aio.AbstractEventLoop:
    def ask_exit() -> None:
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

    aio.ensure_future(main(loop))

    logger.debug('Starting an event loop')
    loop.run_forever()
