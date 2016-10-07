import asyncio
import random

from datetime import datetime
from aiohttp import web


random.seed(1)


async def echo(request):
    name = request.match_info.get("spam", "egg")
    n = datetime.now().isoformat()
    delay = random.randint(0, 3)
    await asyncio.sleep(delay)
    headers = {"content_type": "text/plain", "delay": str(delay)}
    print("{}: {} delay: {}".format(n, request.path, delay))
    response = web.Response(body=bytes(name, 'utf8'), headers=headers)
    return response


def create():
    app = web.Application()
    app.router.add_route("GET", "/{spam}", echo)
    web.run_app(app)


if __name__ == '__main__':
    create()
