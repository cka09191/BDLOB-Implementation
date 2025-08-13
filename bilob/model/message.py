import json
import time
import numpy as np


class Message:
    def __init__(self):
        self.data_np = None
        self.timestamp = None

    @classmethod
    def from_json(cls, json_data: str) -> 'Message':
        """Creates a Message instance from JSON data with efficient numpy conversion."""
        data = json.loads(json_data)
        instance = cls()
        instance.timestamp = time.time()
        
        bids = data.get('b', [])
        asks = data.get('a', [])
        
        bids_array = np.array(bids, dtype=np.float64)
        asks_array = np.array(asks, dtype=np.float64)
        
        depth = len(bids_array)
        if depth == 0:
            return None

        # Create interleaved array using vectorized operations
        combined = np.empty((depth, 4), dtype=np.float64)
        combined[:, :2] = bids_array
        combined[:, 2:] = asks_array
        instance.data_np = combined.ravel()
        return instance

    @classmethod
    def from_dictionary(cls, data: dict) -> 'Message':
        instance = cls()
        instance.timestamp = time.time()
        
        bids = data['bids']
        asks = data['asks']
        
        bids_array = np.array(bids, dtype=np.float64)
        asks_array = np.array(asks, dtype=np.float64)
        
        depth = len(bids_array)
        if depth == 0:
            return None

        # Create interleaved array using vectorized operations
        combined = np.empty((depth, 4), dtype=np.float64)
        combined[:, :2] = bids_array
        combined[:, 2:] = asks_array
        instance.data_np = combined.ravel()

        return instance

