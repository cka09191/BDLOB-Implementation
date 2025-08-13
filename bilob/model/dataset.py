import warnings
import bisect
import inspect
import torch
from torch.utils import data
import numpy as np
from bilob.model.message import Message

class WindowData:
    """
    Manage each sequence to retrieve valid data in window
    """
    def __init__(self, *args):
        """
        Args:
            args: instance of (np.array, torch.Tensor) with dimension (159, -1)
        """

        self.sequences = []
        self.sequence_boundaries = [0]
        cumulative_length = 0

        for arg in args:
            if not isinstance(arg, (np.ndarray, torch.Tensor)):
                raise ValueError(f"Argument {arg} is not an array({type(arg)}")
            if arg.shape[0] != 159:
                raise ValueError(f"Array must have 159 rows, but got {arg.shape[0]}.")
            if not isinstance(arg, torch.Tensor):
                arg = torch.from_numpy(arg)

            cumulative_length += arg.shape[1]

            self.sequences.append(arg)
            self.sequence_boundaries.append(cumulative_length)


    def __len__(self):
        return len(self.sequences)


    def get_trimmed_index(self, left, right):
        """
        Return index to use in dataloader which trimmed each side used
        calculation but don't have smoothed average(left) or
        prediction horizon(right)

        Args:
            left, right: trimming length
        """
        indices = []
        for i in range(len(self.sequences)):
            start = self.sequence_boundaries[i] + left
            end = self.sequence_boundaries[i+1] - right
            if start < end:
                indices.extend(range(start, end))
        return indices


    def get_sequence_index(self, index):
        """
        Return index of sequence that includes the (global) index
        """
        return bisect.bisect_right(self.sequence_boundaries, index) - 1


    def _get_inner_index(self, index, seq_idx):
        return index - self.sequence_boundaries[seq_idx]


    def get_window_sequence(self, seq_idx, inner_index, left, right):
        """
        Return sequence with indicated window size using sequence index

        Args:
            seq_idx: sequence index
            inner_index: sequence element index
            left, right: window length
        """
        sequence = self.sequences[seq_idx]
        if (inner_index-left < 0) or (inner_index+right > sequence.shape[1]):
            raise ValueError(f"Window is larger than available size.")
        return sequence[:, inner_index-left:inner_index+right]


    def get_data_window(self, index, left, right):
        """
        Return sequence with indicated window size using global index

        Args:
            index: global index(element of trimmed index)
            left, right: window length
        """
        seq_idx = self.get_sequence_index(index)
        inner_index = self._get_inner_index(index, seq_idx)
        return self.get_window_sequence(seq_idx, inner_index, left, right)


    def divide_dataset(self, train=60, valid=20, test=20):
        """
        Divide data into train, valid, test set
        Args:
            train, valid, test: percentage of each
        """
        if (train + valid + test) != 100:
            raise ValueError(f"Each size is not valid: train + valid + test = {train} + {valid} + {test} = {test+valid+test}")
        data_size = self.get_data_size()
        last_train = (data_size*train)//100
        last_valid = (data_size*(train+valid))//100

        last_sequence_train = self._get_sequence_number_from_index(last_train)
        last_sequence_valid = self._get_sequence_number_from_index(last_valid)

        if (last_sequence_train == last_sequence_valid) or (last_sequence_valid == len(self)-1):
            raise RuntimeError("Data sequence sizes are not sufficient to divide(too large)")
        return WindowData(*self.sequences[:last_sequence_train]), WindowData(*self.sequences[last_sequence_train:last_sequence_valid]), WindowData(*self.sequences[last_sequence_valid:])


    def get_data_size(self):
        """
        Return total size of the data
        """
        total = 0
        for sequence in self.sequences:
            total+=sequence.shape[1]
        return total


    def _get_sequence_number_from_index(self, index):
        cursor = 0
        number = 0
        for sequence in self.sequences:
            cursor += sequence.shape[1]
            if index < cursor:
               return number
            number +=1


class TimeSeriesDataset(data.Dataset):
    """
    A PyTorch Dataset for times series data with support for multiple sequences,
    customizable time steps, and prediction horizons

    Store information about the available data to retrieve a valid sequence
    """
    def __init__(self, data, prediction_horizon=100, time_steps=100, level = 10, step=1, skip = 1, change=0.1, num_classes=3, smoothing="exponential_sqrt_range", use_difference=True):
        """
        Args:
            data: Input times series data
            prediction_horizon: Number of steps ahead to predict
            time_steps: Number of time steps used for prediction
            level: Feature level limit
            step: Step size for sampling time steps
            skip: Skip between consecutive samples
            change: change of percent to divide label class
            num_classes: number of label classes
            smoothing: exponential, linear, uniform
        """

        self.prediction_horizon = prediction_horizon
        self.time_steps = time_steps
        self.skip = skip
        self.step = step
        self.level = level
        self.change = change/100
        self.num_classes = num_classes

        self.smoothing = smoothing

        self.use_difference = use_difference

        self._data_arrange(data)
        self._process_criteria()
        self._process_index()

        self._precompute_smoothing_weights()
        self._precompute_smoothed_prices()


    def change_properties(self, **kwargs):
        """Manage properties changes to process the inner index"""
        for key, value in kwargs.items():
            if key == 'smoothing':
                self.smoothing = value
                self._precompute_smoothing_weights()
            else:
                setattr(self, key, value)
        self._precompute_smoothed_prices()
        self._process_index()


    def __len__(self):
        """Denotes the total number of samples"""
        return len(self._index_data)


    def __getitem__(self,index):
        x, y, *_ = self._getitems(index)

        return x, y


    def _precompute_smoothing_weights(self):
        size = self.time_steps
        if self.smoothing == 'exponential':
            weights = torch.exp(torch.linspace(0, np.sqrt(size), size))
            self._smooth_weights = weights/torch.sum(weights)
        elif self.smoothing == 'linear':
            weights = torch.linspace(0, 1, size)
            self._smooth_weights = weights/torch.sum(weights)
        elif self.smoothing == 'uniform':
            self._smooth_weights = torch.ones(size)/size
        elif self.smoothing == 'exponential_sqrt_range':
            sqrt_size = int(size**0.5)
            weights = torch.zeros(size)
            weights[-sqrt_size:] = torch.exp(torch.linspace(0, sqrt_size, sqrt_size))

            self._smooth_weights = weights/torch.sum(weights)
        else:
            raise ValueError(f"'{smoothing} is not defined")
        self._smooth_weights = torch.flip(self._smooth_weights, [0])

    def _smoothing(self, x):
        return torch.sum(x * self._smooth_weights, dim=1)


    def _precompute_smoothed_prices(self):
        self.smoothed_prices = []
        self.labels = []
        for sequence in self.data.sequences:
            if sequence.shape[1] < self.time_steps:
                smoothed_price = np.zeros_like(sequence[0,:])
                percentage_change = np.zeros_like(smoothed_price)
            else:
                best_prices = sequence[:4:2,:].numpy()
                mean_price = np.mean(best_prices, axis=0)
                smoothed_price = np.zeros_like(sequence[0,:])
                smoothed_price[self.time_steps-1:] = np.convolve(mean_price, self._smooth_weights, mode='valid')
                smoothed_price_k = np.roll(smoothed_price, -self.time_steps)
                with warnings.catch_warnings(action="ignore"):
                    percentage_change = (smoothed_price_k - smoothed_price)/smoothed_price

            self.smoothed_prices.append(torch.from_numpy(smoothed_price))
            self.labels.append(np.digitize(percentage_change, self.criteria))
        self.smoothed_prices = np.concatenate(self.smoothed_prices)
        self.labels = np.concatenate(self.labels)


    def _data_arrange(self, data):
        if data is None:
            self.data = None
            return
        elif isinstance(data, list):
            self.data = WindowData(*data)
        elif isinstance(data, (np.ndarray, torch.Tensor)):
            self.data = WindowData(data)
        elif isinstance(data, WindowData):
            self.data = data
        else:
            raise ValueError(f"Can't process data type: {type(data)}")


    def _process_index(self):
        """
        Process available index that mapping the real data index
        """
        self._index_data = self.data.get_trimmed_index(self.time_steps, self.prediction_horizon)[::self.skip]


    def _process_criteria(self):
        self.criteria = torch.linspace(-self.change*(self.num_classes//2), self.change*(self.num_classes//2),self.num_classes-1)





    def _getitems(self, indices, test=False):
        """Process multiple items at once for batch efficiency"""
        if isinstance(indices, int):
            real_indices = [self._index_data[indices]]
        else:
            batch_size = len(indices)
            real_indices = [self._index_data[i] for i in indices]


        x_batch = []
        y_batch = []
        if test: sequence_windows = []
        for idx in real_indices:
            x = self.data.get_data_window(idx, self.time_steps, 0)
            y = self.labels[idx]
            if self.use_difference == True:
                mean_last = self.smoothed_prices[idx]
                x = x.clone()
                x[::2,:] -= mean_last
            if test:
                window_item = self.data.get_data_window(idx, self.time_steps, self.prediction_horizon)
                sequence_window = window_item[:self.level*4,:]
                if self.use_difference == True:
                    sequence_window = sequence_window.clone()
                    sequence_window[::2,:]-=mean_last
                sequence_windows.append(sequence_window)

            x_batch.append(x)
            y_batch.append(y)
        x = torch.stack(x_batch)
        x = torch.transpose(x, 1, 2)
        x = x[:,::self.step,:self.level*4]
        y = np.array(y_batch)
        #y = y.squeeze(1)
        if isinstance(indices, int):
            y = y[0]
        if test: return x, y, torch.stack(sequence_windows)
        return x, y

