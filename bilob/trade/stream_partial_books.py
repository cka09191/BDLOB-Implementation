import json
import functools
from bilob.model.message import Message
from binance.websocket.um_futures.websocket_client import UMFuturesWebsocketClient
import asyncio

def _message_handler(_, message, container):
    message = Message.from_json(message)
    if message == None:
        return
    container.append(message)

async def depth_websocket_launch(container, time):
    client = UMFuturesWebsocketClient(
            on_message=functools.partial(_message_handler, container=container)
    )
    client.partial_book_depth(symbol="BTCUSDC", level=20, speed=100)


    await asyncio.sleep(time)

    client.stop()
