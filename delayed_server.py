import asyncio
import random

from datetime import datetime
from aiohttp import web


random.seed(1)

EXAMPLE_FEED = b'''<?xml version="1.0" encoding="UTF-8" ?>
<rss version="2.0">

<channel>
  <title>Example website</title>
  <link>http://www.example.org</link>
  <description>Feed for test purposes</description>
  <item>
    <title>Example item</title>
    <link>http://www.example.org/item1</link>
    <description>First example item</description>
  </item>
  <item>
    <title>Another item</title>
    <link>http://www.example.org/item2</link>
    <description>Second example item</description>
  </item>
</channel>

</rss>
'''


async def rss(request) -> web.Response:
    'Handler responding with a sample RSS feed'

    name = request.match_info.get("spam", "egg")
    n = datetime.now().isoformat()
    delay = random.randint(0, 3)
    await asyncio.sleep(delay)
    print("{}: {} delay: {}".format(n, request.path, delay))
    headers = {"content_type": "application/rss+xml", "delay": str(delay)}
    response = web.Response(body=EXAMPLE_FEED, headers=headers)
    return response


def create_app() -> web.Application:
    app = web.Application()
    app.router.add_route("GET", "/{spam}", rss)
    return app


if __name__ == '__main__':
    app = create_app()
    web.run_app(app)
