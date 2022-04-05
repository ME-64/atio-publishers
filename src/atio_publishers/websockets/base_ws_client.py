from abc import ABC, abstractmethod
import ujson
import logging
import sys
import asyncio
import aiohttp
from redis import asyncio as aioredis
import aioprocessing as aiop
import multiprocessing as mp
import os
import signal

log: logging.Logger = logging.getLogger('atio')


class Publisher:

    def __init__(self, redis_url: str, redis_channel: str, pub_queue: aiop.Queue):# {{{
        self._started: asyncio.Event = asyncio.Event()
        self.redis_url: str = redis_url
        self.redis_channel: str = redis_channel
        self.pub_queue: aiop.Queue = pub_queue# }}}

    async def _start(self) -> None:# {{{
        self.redis: aioredis.client.Redis = aioredis.from_url(self.redis_url, decode_responses=True)
        await self.redis.ping()
        self._started.set()
        while True:
            try:
                to_pub: dict = await self.pub_queue.coro_get()
                await self.redis.publish(self.redis_channel, ujson.dumps(to_pub))
            except:
                os.kill(os.getpid(), signal.SIGINT)# }}}

    async def _run(self) -> None:# {{{
        loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.create_task(self._start())# }}}

    def start(self) -> None:# {{{
        self.proc: mp.Process = mp.Process(target=self._run)
        self.proc.start()# }}}

class Worker(ABC):

    def __init__(self, work_queue: aiop.Queue, pub_queue: aiop.Queue):# {{{
        self.work_queue: aiop.Queue = work_queue
        self.pub_queue: aiop.Queue = pub_queue# }}}

    @abstractmethod
    def do_work(self, work: dict) -> dict:# {{{
        pass# }}}

    def _run(self) -> None:# {{{
        while True:
            try:
                work  = self.work_queue.get()
                result: dict = self.do_work(work)
                self.pub_queue.put(result)
            except:
                os.kill(os.getpid(), signal.SIGINT)# }}}

    def start(self) -> None:# {{{
        self.proc: mp.Process = mp.Process(target=self._run)
        self.proc.start()# }}}

class BaseWSClient(ABC):

    def __init__(self, ws_url: str, redis_url: str, redis_channel: str,#{{{
            worker: Worker, publisher: Publisher):
        self.ws_url: str = ws_url
        self._started: asyncio.Event = asyncio.Event()
        self.pub_queue: aiop.Queue = aiop.Queue()
        self.work_queue: aiop.Queue = aiop.Queue()
        self.publisher: Publisher = publisher(redis_url=redis_url,
                redis_channel=redis_channel,
                pub_queue=self.pub_queue)
        self.worker: Worker = worker(self.work_queue, self.pub_queue)# }}}

    @abstractmethod
    async def subscribe(self, *args, **kwargs):# {{{
        pass# }}}

    @abstractmethod
    async def on_start(self):# {{{
        pass# }}}

    @abstractmethod
    async def on_message(self, msg: aiohttp.WSMessage) -> None:# {{{
        pass# }}}

    async def start(self) -> None:# {{{
        self.session: aiohttp.ClientSession = aiohttp.ClientSession()
        self.ws: aiohttp.client_ws.ClientWebSocketResponse = await self.session.ws_connect(self.ws_url)
        self.publisher.start()
        self.worker.start()
        self._started.set()
        await self.on_start()

        while not self.ws.closed:
            msg: aiohttp.WSMessage = await self.ws.receive()
            if msg.type == aiohttp.WSMsgType.TEXT:
                await self.on_message(msg)
            else:
                log.debug(f'msg received: {msg}')
        # when the websocket connection is closed, we disconnect
        # and let the container handle reconnecting
        os.kill(os.getpid(), signal.SIGINT)# }}}
