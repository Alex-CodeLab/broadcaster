import typing
from urllib.parse import urlparse
from .base import BroadcastBackend
from .._base import Event
import zmq
import zmq.asyncio
import asyncio


class ZmqBackend(BroadcastBackend):

    def __init__(self, url: str):
        parsed_url = urlparse(url)
        self._host = parsed_url.hostname or "127.0.0.1"
        self._port = parsed_url.port or 5555
        self.loop = asyncio.get_event_loop()
        self._consumer_channels: typing.Set = set()
        self.ctx_pub = zmq.asyncio.Context()
        self.ctx_sub = zmq.asyncio.Context()

    async def connect(self) -> None:
        self.sock_pub = self.ctx_pub.socket(socket_type=zmq.PUB)
        self.sock_sub = self.ctx_sub.socket(socket_type=zmq.SUB)
        self.sock_pub.bind(f"tcp://{self._host}:self._port")
        self.sock_sub.setsockopt(zmq.RCVHWM, 0)
        self.sock_sub.connect(f"tcp://{self._host}:self._port")
        self.loop.create_task(coro=self.next_published())

    async def disconnect(self) -> None:
        self.ctx_pub.destroy()
        self.ctx_sub.destroy()

    async def subscribe(self, channel: str) -> None:
        self._consumer_channels.add(channel)
        self.sock_sub.setsockopt_string(zmq.SUBSCRIBE, channel)

    async def unsubscribe(self, channel: str) -> None:
        self.sock_sub.setsockopt_string(zmq.UNSUBSCRIBE, channel)

    async def publish(self, channel: str, message: typing.Any) -> None:
        await self.sock_pub.send_multipart(msg_parts=[channel.encode("utf8"), message.encode("utf8")])

    async def next_published(self) -> Event:
        topic, body = await self.sock_sub.recv_multipart()

        # schedule ourselves to receive the next message
        asyncio.ensure_future(self.next_published())

        return Event(channel=topic, message=body)

