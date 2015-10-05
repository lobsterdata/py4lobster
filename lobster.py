__author__ = 'Ruihong Huang'

from pandas import DataFrame, read_csv, concat
from os import path
import numpy as np


class Lobster:
    def __init__(self):
        self.order_books = None
        self.messages = DataFrame()

    def read_files(self, order_book_file, message_file, append=True):
        file_name = path.basename(order_book_file)
        file_base = path.splitext(file_name)[0]
        mydate = np.datetime64('{0}T00:00-0000'.format(file_base.split('_')[1]))

        myorderbooks = read_csv(order_book_file, header=None)
        mymessages = read_csv(message_file, header=None)
        columns = []
        for iCol in range(len(myorderbooks.columns) / 4):
            columns.append('Ask_Price_{0}'.format(iCol + 1))
            columns .append('Ask_Size_{0}'.format(iCol + 1))
            columns .append('Bid_Price_{0}'.format(iCol + 1))
            columns .append('Bid_Size_{0}'.format(iCol + 1))
        myorderbooks.columns = columns
        mymessages.columns = ['Time', 'Event', 'Order_ID', 'Size', 'Price', 'Direction']
        mymessages.Time *= 1e9
        timestamps = []
        for iRow in range(mymessages.shape[0]):
            timestamps.append(mydate + np.timedelta64(int(mymessages.Time[iRow]), 'ns'))

        myorderbooks.index = timestamps
        mymessages.index = timestamps

        if append:
            self.order_books = concat([self.order_books,myorderbooks], axis=0)
            self.messages = concat([self.messages,mymessages], axis=0)
        else:
            self.order_books = myorderbooks
            self.messages = mymessages

    def get_number_of_record(self):
        return self.order_books.shape[0]

    def get_number_of_level(self):
        return int(self.order_books.shape[1] / 4 + 0.5)

    def bind_trades(self):
        pass

    def plot(self):
        pass

