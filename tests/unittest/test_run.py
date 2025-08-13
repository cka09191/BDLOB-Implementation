from tqdm import tqdm  # Use async-compatible tqdm
from bilob.trade.stream_partial_books import depth_websocket_launch
from bilob.model.books_deque import BooksDeque
from bilob.model.strategy import strategy
from bilob.utils.get_api_key import get_api_key
from bilob.utils.get_position import get_position
from binance.um_futures import UMFutures


import torch
import asyncio
import time
import numpy as np

api_key, api_secret = get_api_key()

um_futures_client = UMFutures(key=api_key, secret=api_secret)

async def _prediction_stream(_books_deque, time_run):
    print()
    print('weight load')
    weight_path = './tests/resources/BayesianDeepLOB_weights_exp_0406_custom_criteria_e_1s_k_4500_skip1_1e-06_zscore_each_20_criteria.pth'
    model = torch.load(weight_path, map_location = torch.device('cpu'), weights_only=False) 
    model.device = torch.device('cpu')
    print()
    print('waiting until deque is full')
    pbar = tqdm(total = 4500)
    while(not _books_deque.is_full()):
        await asyncio.sleep(1)
        pbar.n = len(_books_deque)
        pbar.refresh()
    pbar.close()
    print()
    print('start prediction')
    time_start = time.time()
    time_elapsed = 0.0
    time_record = []
    prediction_record = []
    prediction_softmax_record = []
    mid_record = []
    while(time_elapsed<time_run):
        print()
        time_elapsed = time.time() - time_start
        feature, mid_last = _books_deque.get_feature()
        with torch.no_grad():
            model.train(False)
            feature = feature.tile((100,1,1,1))
            prediction_softmax = model(feature)
            predictions = torch.max(prediction_softmax,1)[1].cpu().detach().numpy()[0]
            prediction = np.mean(
            prediction_softmax = prediction_softmax.cpu().detach().numpy()[0]
        prediction_softmax_record.append(prediction_softmax[0])
        prediction_record.append(prediction)
        time_record.append(time_elapsed)
        mid_price = mid_last
        mid_record.append(mid_price)
        print(f'{mid_price:.1f}', end=' ')
        for e in prediction_softmax:
            print(f'{e:.2f},', end=' ')
        signal = (prediction_softmax[2]-prediction_softmax[0])/max(prediction_softmax[1], 0.2)
        print(f'{signal:.4f}')

        print(f'position:{get_position(um_futures_client)}')
        strategy(um_futures_client, prediction_softmax)

        await asyncio.sleep(500)
    

def test_prediction_stream():
    print(f'position:{get_position(um_futures_client)}')
    books_deque = BooksDeque(4500)
    async def main():
        await asyncio.gather(depth_websocket_launch(books_deque, 10000),
        _prediction_stream(books_deque, 5000))

    asyncio.run(main())
    
