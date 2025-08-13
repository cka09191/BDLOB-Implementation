from bilob.model.data_deque import DataDeque
import numpy as np

class PredictionsDeque(DataDeque):
    def __init__(self,maxlen):
        super().__init__(maxlen)

    def get_numpy_array(self):
        items = self.get_all()
        return np.stack(items)

    def append(self, item):
        if not isinstance(item, np.ndarray): 
            raise ValueError(f'{type(item)} is not available')
        if not item.shape == (3):
            raise ValueError(f'shape({item.shape}) is not (3)')
        super().append(item)
