__author__ = 'Ruihong Huang'
from datetime import datetime
from lobster import LobsterData


def test_read_single_date_files():
    book_file = 'sample_data/AMZN_2012-06-21_34200000_57600000_orderbook_10.csv'
    message_file = 'sample_data/AMZN_2012-06-21_34200000_57600000_message_10.csv'

    lob = LobsterData()
    lob.read_single_day_data(order_book_file=book_file, message_file=message_file)
    print(lob.get_number_of_record())


def test_read_period_data():
    directory = '/home/ruihong/lobstertest'
    ticker = 'AMZN'
    start_date = datetime(2015, 10, 5)
    end_date = datetime(2015, 10, 9)
    level = 10

    lob = LobsterData()
    lob.read_period_data(ticker, level, start_date, end_date, data_path=directory)
    print(lob.get_number_of_record())


test_read_period_data()
