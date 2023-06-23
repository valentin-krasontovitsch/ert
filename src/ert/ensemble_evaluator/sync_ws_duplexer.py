import asyncio
import os
import ssl
import threading
import time
from concurrent.futures import CancelledError
from typing import Iterator, Optional, Union

import websockets
from cloudevents.http import CloudEvent
from websockets.client import WebSocketClientProtocol  # type: ignore
from websockets.datastructures import Headers

from ._wait_for_evaluator import wait_for_evaluator


class SyncWebsocketDuplexer:
    """Class for communicating bi-directionally with a websocket using a
    synchronous API. Reentrant, but not thread-safe. One must call stop() after
    communication ends."""

    def __init__(
        self,
        uri: str,
        health_check_uri: str,
        cert: Union[str, bytes, os.PathLike],
        token: str,
    ) -> None:
        self._uri = uri
        self._hc_uri = health_check_uri
        self._token = token
        self._extra_headers = Headers()
        if token is not None:
            self._extra_headers["token"] = token

        # Mimics the behavior of the ssl argument when connection to
        # websockets. If none is specified it will deduce based on the url,
        # if True it will enforce TLS, and if you want to use self signed
        # certificates you need to pass an ssl_context with the certificate
        # loaded.
        self._cert = cert
        ssl_context: Optional[Union[bool, ssl.SSLContext]] = None
        if cert is not None:
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ssl_context.load_verify_locations(cadata=cert)
        else:
            ssl_context = True if self._uri.startswith("wss") else None
        self._ssl_context: Optional[Union[bool, ssl.SSLContext]] = ssl_context

        self._loop = asyncio.new_event_loop()
        self._connection: asyncio.Task = self._loop.create_task(self._connect())
        self._ws: Optional[WebSocketClientProtocol] = None
        self._loop_thread = threading.Thread(target=self._loop.run_forever)
        self._loop_thread.start()

        # Ensure the async thread either makes a connection, or raises the _connect()
        # exception before returning. Not before a connection has been made, can this
        # class be used safely.
        while not self._connection.done():
            time.sleep(0.1)
        try:
            self._connection.result()
        except Exception:
            self.stop()
            raise

    async def _connect(self) -> None:
        connect = websockets.client.connect(
            self._uri,
            ssl=self._ssl_context,
            extra_headers=self._extra_headers,
            max_size=2**26,
            max_queue=500,
            open_timeout=60,
            ping_timeout=None,
            ping_interval=60,
            close_timeout=60,
        )

        await wait_for_evaluator(
            base_url=self._hc_uri,
            token=self._token,
            cert=self._cert,
        )

        self._ws = await connect

    def _ensure_running(self) -> None:
        try:
            asyncio.run_coroutine_threadsafe(
                asyncio.wait_for(self._connection, None),
                loop=self._loop,
            ).result()
        except OSError:
            self.stop()
            raise
        if not self._ws:
            raise RuntimeError("was connected but _ws was not set")

    def send(self, msg: str) -> None:
        """Send a message."""
        self._ensure_running()
        try:
            asyncio.run_coroutine_threadsafe(
                self._ws.send(msg), loop=self._loop  # type: ignore
            ).result()
        except OSError:
            self.stop()
            raise

    def receive(self) -> Iterator[CloudEvent]:
        """Create a generator with which you can iterate over incoming
        websocket messages."""
        self._ensure_running()
        while True:
            try:
                event = asyncio.run_coroutine_threadsafe(
                    self._ws.recv(), loop=self._loop  # type: ignore
                ).result()
                yield event
            except OSError:
                self.stop()
                raise

    def stop(self) -> None:
        """Stop the duplexer. Most likely idempotent."""
        try:
            if self._loop.is_running():
                if self._ws:
                    asyncio.run_coroutine_threadsafe(
                        self._ws.close(), loop=self._loop
                    ).result()
                try:
                    self._loop.call_soon_threadsafe(self._connection.cancel)
                    asyncio.run_coroutine_threadsafe(
                        asyncio.wait_for(self._connection, None),
                        loop=self._loop,
                    ).result()
                except (OSError, asyncio.CancelledError, CancelledError):
                    # The OSError will have been raised in send/receive already.
                    pass
        except Exception:
            pass
        finally:
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._loop_thread.join()
