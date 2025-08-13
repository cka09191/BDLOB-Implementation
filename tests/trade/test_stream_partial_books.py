import asyncio
from bilob.trade.stream_partial_books import depth_websocket_launch
from bilob.model.books_deque import BooksDeque

async def inspect_depth(_books_deque):
    await asyncio.sleep(1)
    print(_books_deque.get_all()[0].data_np)
    print(len(_books_deque.get_all()))
    await asyncio.sleep(1)
    print(len(_books_deque.get_all()))
    await asyncio.sleep(1)
    feature, _ = _books_deque.get_feature()
    print(feature.shape)
    print(feature)
    

def test_depth_websocket_launch():
    _books_deque = BooksDeque(100)

    async def _tasks():
        await asyncio.gather(
            depth_websocket_launch(_books_deque,2),
            inspect_depth(_books_deque)
            )
    
    asyncio.run(_tasks())
