__author__ = 'Ruihong Huang'

from lobster import Lobster


def test_read_files():
    book_file = 'sample_data/AMZN_2015-07-02_34200000_57600000_orderbook_5.csv'
    message_file = 'sample_data/AMZN_2015-07-02_34200000_57600000_message_5.csv'

    lob = Lobster()
    lob.read_files(order_book_file=book_file, message_file=message_file)
    print(lob.get_number_of_record())


test_read_files()

