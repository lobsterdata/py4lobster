__author__ = 'Ruihong Huang'

from pandas import DataFrame, read_csv, concat
from os import path
import numpy as np
from datetime import timedelta
from enum import Enum

class OrderEvent(Enum):
    SUBMISSION = 1
    CANCELLATION = 2
    DELETION = 3
    EXECUTION = 4
    HIDDEN_EXECUTION =5
    CROSS_TRADE = 6
    TRADING_HALT  = 7
    OTHER = 8

__EventMap = {}

for e in OrderEvent:
    __EventMap[e.value] = e

def get_orderEvent(eventid):
    return __EventMap[eventid]


class LobsterData:
    def __init__(self):
        self.order_books = None
        self.messages = DataFrame()
        self.level = 0

    def read_single_day_data(self, order_book_file, message_file, append=True, convert_time=False):
        file_name = path.basename(order_book_file)
        file_base = path.splitext(file_name)[0]
        mydate = np.datetime64('{0}T00:00-0000'.format(file_base.split('_')[1]))

        myorderbooks = read_csv(order_book_file, header=None)
        mymessages = read_csv(message_file, header=None)
        columns = []
        for iCol in range(int(len(myorderbooks.columns) / 4 + 0.5)):
            columns.append('Ask_Price_{0}'.format(iCol + 1))
            columns.append('Ask_Size_{0}'.format(iCol + 1))
            columns.append('Bid_Price_{0}'.format(iCol + 1))
            columns.append('Bid_Size_{0}'.format(iCol + 1))
        myorderbooks.columns = columns
        mymessages.columns = ['Time', 'Event', 'Order_ID', 'Size', 'Price', 'Direction']
        mymessages.Event = mymessages['Event'].map(get_orderEvent)
        mymessages['Date'] = mydate
        if convert_time:
            mymessages.Time = mymessages.Time.apply(lambda x: mydate + np.timedelta64(int(x * 1e9), 'ns'))

        mymessages.set_index(['Date', 'Time'], inplace=True)
        myorderbooks.index = mymessages.index

        if append:
            self.order_books = concat([self.order_books, myorderbooks], axis=0)
            self.messages = concat([self.messages, mymessages], axis=0)
        else:
            self.order_books = myorderbooks
            self.messages = mymessages
        self.level = int(self.order_books.shape[1] / 4 + 0.5)

    def read_period_data(self, ticker, level, start_date, end_date, data_path='./', **kwargs):
        my_date = start_date
        while my_date <= end_date:
            print('read {0}'.format(my_date.strftime('%Y-%m-%d')))
            order_book_file = "{0}/{1}_{2}_34200000_57600000_orderbook_{3}.csv".format(data_path, ticker,
                                                                                       my_date.strftime('%Y-%m-%d'),
                                                                                       level)

            message_file = "{0}/{1}_{2}_34200000_57600000_message_{3}.csv".format(data_path, ticker,
                                                                                       my_date.strftime('%Y-%m-%d'),
                                                                                       level)
            self.read_single_day_data(order_book_file, message_file, **kwargs)
            my_date += timedelta(1)

    def get_number_of_record(self):
        return self.order_books.shape[0]

    def get_number_of_level(self):
        return self.level

    def get_trades(self, combined=True, combination_interval=100):
        pass
    
    def exclude_trade_halt_data(self):
        halt_indicators = where(self.message.event == 7)
        
    def plot_orderbook(self):
        pass
