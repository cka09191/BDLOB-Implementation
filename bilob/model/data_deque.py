from collections import deque
import threading

class DataDeque:
    def __init__(self,maxlen):
        self.maxlen = maxlen
        self.deque = deque(maxlen=maxlen)
        self.lock = threading.Lock() 

    def append(self, item):
        with self.lock:
            self.deque.append(item)

    def get_all(self):
        with self.lock:
            items = list(self.deque)
            return items

    def is_full(self):
        if len(self.deque) == self.maxlen:
            return True
        else:
            return False
    
    def __len__(self):
        return len(self.deque)
