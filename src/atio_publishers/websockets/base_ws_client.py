from abc import ABC, abstractmethod
import ujson
import logging
import sys
import asyncio
import aiohttp
from redis import asyncio as aioredis
import aioprocessing as aiop
from aioprocessing import queues
import multiprocessing as mp
import os
import signal

log: logging.Logger = logging.getLogger('atio')


AioQ = queues.AioQueue


class Publisher:

    def __init__(self, redis_url: str, redis_channel: str, pub_queue: AioQ):# {{{
        self._started: aiop.AioEvent = aiop.AioEvent()
        self.redis_url: str = redis_url
        self.redis_channel: str = redis_channel
        self.pub_queue: AioQ = pub_queue
        log.debug('publisher init complete')
        # }}}

    async def _start(self) -> None:# {{{
        log.debug('starting publisher...')
        self.redis_pool = aioredis.ConnectionPool.from_url(url=self.redis_url, max_connections=10)
        self.redis: aioredis.client.Redis = aioredis.Redis(connection_pool=self.redis_pool, decode_responses=True)
        await self.redis.ping()
        log.debug('publisher -> redis connection is running')
        self._started.set()
        log.debug('publisher event loop is running')
        while True:
            try:
                to_pub: dict = await self.pub_queue.coro_get()
                if to_pub:
                    await self.redis.publish(self.redis_channel, ujson.dumps(to_pub))
            except Exception as e:
                log.critical(f'error received in publisher thread {e}')
                self._started.clear()
                break
                # }}}

    def _run(self) -> None:# {{{
        loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.create_task(self._start())
        loop.run_forever()
        # }}}

    def start(self) -> None:# {{{
        log.debug('publisher .start() method called')
        self.proc: mp.Process = mp.Process(target=self._run, daemon=True)
        self.proc.start()
        log.debug('publisher .start() message complete')
        # }}}

class Worker(ABC):

    def __init__(self, work_queue: AioQ, pub_queue: AioQ):# {{{
        self.work_queue: AioQ = work_queue
        self.pub_queue: AioQ = pub_queue
        self._started: aiop.AioEvent = aiop.AioEvent()
        log.debug('worker init complete')
        # }}}

    @abstractmethod
    def do_work(self, work: dict) -> dict:# {{{
        pass# }}}

    def _run(self) -> None:# {{{
        self._started.set()
        log.debug('worker event loop started...')
        while True:
            try:
                work  = self.work_queue.get()
                log.debug('worker received some work...')
                result: dict = self.do_work(work)
                log.debug('worker work done')
                self.pub_queue.put(result)
            except:
                self._started.clear()
                break
                # }}}

    def start(self) -> None:# {{{
        log.debug('worker .start() method called')
        self.proc: mp.Process = mp.Process(target=self._run, daemon=True)
        self.proc.start()
        log.debug('worker .start() method complete')
        # }}}

class BaseWSClient(ABC):

    def __init__(self, ws_url: str, redis_url: str, redis_channel: str,#{{{
            worker: Worker, publisher: Publisher):
        self.ws_url: str = ws_url
        self._started: aiop.AioEvent = aiop.AioEvent()
        self.pub_queue: AioQ = aiop.AioQueue()
        self.work_queue: AioQ = aiop.AioQueue()
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
        log.debug('starting the publisher and worker')
        self.publisher.start()
        self.worker.start()
        await self.publisher._started.coro_wait()
        await self.worker._started.coro_wait()
        log.debug('publisher and worker started')
        log.debug('basewsclient .start() method called')
        self.session: aiohttp.ClientSession = aiohttp.ClientSession()
        self.ws: aiohttp.client_ws.ClientWebSocketResponse = await self.session.ws_connect(self.ws_url)
        log.debug('websocket connect established')
        self._started.set()
        await self.on_start()

        log.debug('websocket event loop starting')
        while not self.ws.closed and self.publisher._started.is_set() and self.worker._started.is_set():
            try:
                msg: aiohttp.WSMessage = await asyncio.wait_for(self.ws.receive(), timeout=5)
            except Exception as e:
                log.critical('timeout on websocket connection, shutting down')
                sys.exit(1)
            if msg.type == aiohttp.WSMsgType.TEXT:
                await self.on_message(msg)
            else:
                log.debug(f'msg received: {msg}')
        # when the websocket connection is closed, we disconnect
        # and let the container handle reconnecting
        sys.exit(1) # }}}
