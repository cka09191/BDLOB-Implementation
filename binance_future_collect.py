import time
import os
import json
import asyncio
import websockets
import pandas as pd
import boto3
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# Configuration
BUFFER_DIR = ''
S3_BUCKET = ''
BUFFER_LIMIT = 5  # MB
SYMBOL = 'btcusdc'
TEST_MODE = False
class MessageProcessor:
    def process_message(self, message):
        data = json.loads(message)
        if 'e' in data and data['e'] == "depthUpdate":
            # Process initial depth snapshot
            timestamp_now = time.time()
            last_update_id = data.get('u')
            bids = data.get('b')
            asks = data.get('a')
            data =  ''
            for bid, ask in zip(bids, asks):
                data += f"{float(bid[0])},{float(bid[1])},{float(ask[0])},{float(ask[1])}\n"
            return {
                'timestamp': timestamp_now,
                'last_update_id': last_update_id,
                'data': data
            }
        else:
            return None

class BufferManager:
    def __init__(self, buffer_dir):
        self.depth_buffer = []
        self.current_file_depth = None
        self.buffer_dir = buffer_dir
        self.setup_directories()

    def setup_directories(self):
        os.makedirs(self.buffer_dir, exist_ok=True)
        self.current_file_depth = f"depth_lob_future_data_{int(datetime.now().timestamp())}.parquet"

    def add_to_buffer(self, data):
        if data:
            self.depth_buffer.append(data)

    async def flush_buffer(self, file_uploader):
        if not self.depth_buffer:
            return

        if self.depth_buffer:
            df_depth = pd.DataFrame(self.depth_buffer)
            path_depth = os.path.join(self.buffer_dir, self.current_file_depth)
            print(f"Flushing depth buffer to file: {path_depth}")
            if os.path.exists(path_depth):
                existing = pd.read_parquet(path_depth)
                df_depth = pd.concat([existing, df_depth])
            df_depth.to_parquet(path_depth, index=False)
            self.depth_buffer.clear()

            file_size_depth = os.path.getsize(path_depth) / (1024 ** 2)
            if TEST_MODE or file_size_depth >= BUFFER_LIMIT:
                await file_uploader.upload(path_depth, 'depth')

                self.current_file_depth = f"depth_lob_future_data_{int(datetime.now().timestamp())}.parquet"

class FileUploader:
    def __init__(self, s3_bucket):
        self.session = boto3.Session()
        self.s3 = self.session.client('s3', region_name='ap-southeast-2')
        self.s3_bucket = s3_bucket
        self.executor = ThreadPoolExecutor(max_workers=2)

    async def upload(self, path, file_type):
        loop = asyncio.get_event_loop()
        file_name = os.path.basename(path)
        try:
            print(f"Starting upload for {file_name}...")
            await loop.run_in_executor(
                self.executor,
                self._s3_upload,
                path,
                file_name,
                file_type
            )
            print(f"Uploaded {file_name} to S3")
        except Exception as e:
            print(f"Upload failed: {str(e)}")

    def _s3_upload(self, path, file_name, file_type):
        extra_args = {
            'StorageClass': 'STANDARD_IA',
            'ACL': 'private'
        }
        with open(path, 'rb') as f:
            self.s3.upload_fileobj(
                f,
                self.s3_bucket,
                f"binance-ws/{datetime.now().strftime('%Y%m%d')}/{file_type}/{file_name}",
                ExtraArgs=extra_args
            )
        os.remove(path)

async def websocket_client(message_processor, buffer_manager):
    uri = "wss://fstream.binance.com/ws"
    streams = [
        f"{SYMBOL}@depth20@100ms"
    ]
    while True:
        async with websockets.connect(uri, ping_interval=60, ping_timeout=50) as websocket:
            subscribe_message = {
                "method": "SUBSCRIBE",
                "params": streams,
                "id": 1
            }
            
            await websocket.send(json.dumps(subscribe_message))
            
            # Handle subscription responses
            for _ in range(len(streams)):
                response = await websocket.recv()
                print(f"Subscription response: {response}")
            
            # Process incoming messages
            async for message in websocket:
                data = message_processor.process_message(message)
                if data:
                    buffer_manager.add_to_buffer(data)
                    #print(data)
                    if len(buffer_manager.depth_buffer) >= 100:
                        file_uploader = FileUploader(S3_BUCKET)
                        await buffer_manager.flush_buffer(file_uploader)

async def periodic_flush(buffer_manager):
    while True:
        await asyncio.sleep(300)  # 5 minutes
        file_uploader = FileUploader(S3_BUCKET)
        await buffer_manager.flush_buffer(file_uploader)

async def main():
    message_processor = MessageProcessor()
    buffer_manager = BufferManager(BUFFER_DIR)
    
    if TEST_MODE:
        await websocket_client(message_processor, buffer_manager)
    else:
        await asyncio.gather(
            websocket_client(message_processor, buffer_manager),
            periodic_flush(buffer_manager)
        )

if __name__ == "__main__":
    import logging
    logging.getLogger("websockets").setLevel(logging.INFO)
    boto3.set_stream_logger('')
    while True:
        try:
            asyncio.run(main())
        except Exception as e:
            print(e)
            time.sleep(5)
