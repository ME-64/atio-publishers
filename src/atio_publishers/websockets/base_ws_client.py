from abc import ABC, abstractmethod
import ujson
import time
import logging
import sys
import asyncio
import aiohttp
from redis import asyncio as aioredis # type: ignore
import aioprocessing as aiop
import multiprocessing as mp
from typing import Type
from typing_extensions import TypedDict

log: logging.Logger = logging.getLogger('atio')




class Publisher:

    def __init__(self, redis_url: str, redis_channel: str, pub_queue):# {{{
        self._started = aiop.AioEvent()
        self.redis_url: str = redis_url
        self.redis_channel: str = redis_channel
        self.pub_queue = pub_queue
        log.debug('publisher init complete')
        # }}}

    async def _start(self) -> None:# {{{
        log.debug('starting publisher...')
        self.redis_pool = aioredis.ConnectionPool.from_url(url=self.redis_url, max_connections=10)
        self.redis: aioredis.client.Redis = aioredis.Redis(connection_pool=self.redis_pool, decode_responses=True)
        await self.redis.ping()
        log.debug('publisher -> redis connection is running')
        self._started.set() # type: ignore
        log.debug('publisher event loop is running')
        while True:
            try:
                to_pub: dict = await self.pub_queue.coro_get()
                if to_pub:
                    await asyncio.wait_for(self.redis.publish(self.redis_channel, ujson.dumps(to_pub)), timeout=5)
            except TimeoutError as e:
                log.critical(f'error received in publisher thread {e}')
                self._started.clear()# type: ignore
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

    def __init__(self, work_queue, pub_queue):# {{{
        self.work_queue = work_queue
        self.pub_queue = pub_queue
        self._started = aiop.AioEvent()
        log.debug('worker init complete')
        # }}}

    @abstractmethod
    def do_work(self, work: dict) -> dict:# {{{
        pass# }}}

    def _run(self) -> None:# {{{
        self._started.set() # type: ignore
        log.debug('worker event loop started...')
        while True:
            try:
                work  = self.work_queue.get()
                log.debug('worker received some work...')
                result: dict = self.do_work(work)
                log.debug('worker work done')
                self.pub_queue.put(result)
            except:
                self._started.clear() # type: ignore
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
            worker: Type[Worker], publisher: Type[Publisher]):
        self.ws_url: str = ws_url
        self._started = aiop.AioEvent()
        self.pub_queue = aiop.AioQueue()
        self.work_queue = aiop.AioQueue()
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

    async def _start(self) -> None:# {{{
        log.debug('starting the publisher and worker')
        self.publisher.start()
        self.worker.start()
        await self.publisher._started.coro_wait() # type: ignore
        await self.worker._started.coro_wait() # type: ignore
        log.debug('publisher and worker started')
        log.debug('basewsclient .start() method called')
        self.session: aiohttp.ClientSession = aiohttp.ClientSession()
        self.ws: aiohttp.ClientWebSocketResponse = await self.session.ws_connect(self.ws_url)
        log.debug('websocket connect established')
        self._started.set() # type: ignore
        await self.on_start()

        log.debug('websocket event loop starting')
        while not self.ws.closed and self.publisher._started.is_set() and self.worker._started.is_set(): # type: ignore
            try:
                msg: aiohttp.WSMessage = await asyncio.wait_for(self.ws.receive(), timeout=5)
            except:
                log.critical('timeout on websocket connection, shutting down')
                sys.exit(1)
            if msg.type == aiohttp.WSMsgType.TEXT:
                await self.on_message(msg)
            else:
                log.debug(f'msg received: {msg}')
        # when the websocket connection is closed, we disconnect
        # and let the container handle reconnecting
        self._started.clear()  # type: ignore
        # }}}

    def start(self) -> None:# {{{
        loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
        loop.run_until_complete(self._start())# }}}


class WSDict(TypedDict):
    client: Type[BaseWSClient]# {{{
    failed: bool
    numb_retries: int
    process: mp.Process# }}}


class WSManager:

    def __init__(self, ws_clients: list[Type[BaseWSClient]], max_retries: int = 5):# {{{
        """intialise each websocket client and their retries"""
        self.max_retries: int = max_retries
        self.ws_clients: dict[int, WSDict]  = {}
        self.complete_failure: bool = False

        for i, wsc in enumerate(ws_clients):
            self.ws_clients[i] = {
                    'client': wsc,
                    'failed': False,
                    'numb_retries': 0,
                    'process': mp.Process(target=wsc.start, daemon=True)
                    }# }}}

    def intialise_clients(self):# {{{
        for cid, client in self.ws_clients.items():
            log.debug(f'starting client with {cid}')
            client['process'].start()
            client['client']._started.wait() # type: ignore }}}

    def kill_clients(self):# {{{
        for client in self.ws_clients.values():
            client['process'].terminate()# }}}

    def check_fail(self):# {{{
        for client in self.ws_clients.values():
            if client['client']._started.is_set() == False: # type: ignore
                client['failed'] = True# }}}

    def restart_failed(self):# {{{
        for client in self.ws_clients.values():
            if client['failed'] == False:
                continue

            if client['numb_retries'] >= self.max_retries:
                log.critical(f'a client has failed and gone over maximum restarts, exiting')
                self.kill_clients()
                self.complete_failure = True
            else:
                client['process'].terminate()
                time.sleep(client['numb_retries'] * 3)
                log.debug('sleeping before retrying client')
                client['numb_retries'] += 1
                client['process'] = mp.Process(target=client['client'].start, daemon=True)
                client['process'].start()# }}}

    def run(self):# {{{
        self.intialise_clients()
        while not self.complete_failure:
            self.check_fail()
            self.restart_failed()# }}}



