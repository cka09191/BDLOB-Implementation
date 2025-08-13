from bilob.model.message import Message
from bilob.model.data_deque import DataDeque
import numpy as np
import torch

class BooksDeque(DataDeque):
    def __init__(self,maxlen):
        super().__init__(maxlen)

    def get_feature(self, use_ratio=True, get_mid_last=True):
        items = self.get_all()
        books = []
        for message in items:
            books.append(message.data_np)

        feature = torch.from_numpy(np.stack(books))

        if get_mid_last:
            mid_last = (feature[-1,0]+feature[-1,2])/2

        if use_ratio:
            mid_first = (feature[0,0] + feature[0,2])/2
            feature[:,::2] -= mid_first
        
        feature = feature.unsqueeze(0).unsqueeze(0).type(torch.float)
        
        if get_mid_last:
            return feature, mid_last

        return feature

    def append(self, item):
        if not isinstance(item, Message): 
            raise ValueError(f'{type(item)} is not available')
        super().append(item)

       
