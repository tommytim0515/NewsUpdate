import datetime
import time
import json
import queue
import threading
import logging
import logging.handlers
import tushare as ts
import pandas as pd
import pipeline
from functions import (iterate_date, STRING_FORMAT,
                       DATA_SOURCES, TOKEN_INDEX, SLEEP_INTERVAL, ERROR_INTERVAL)


class Tushare:
    def __init__(self, data_sources=DATA_SOURCES):
        '''Initialization of tushare.

        Load tokens from json file and initialize the pro api according to the number of the tokens.
        '''
        self._logger = logging.getLogger(__name__)
        file_handler = logging.handlers.RotatingFileHandler(
            '/home/ubuntu/Desktop/tommy/PyFina/news_update/logs/tushare.log', maxBytes=1024*1024, backupCount=3)
        file_handler.setLevel(logging.INFO)
        file_format = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_format)
        self._logger.addHandler(file_handler)
        try:
            with open('/home/ubuntu/Desktop/tommy/PyFina/news_update/tokens.json') as json_file:
                self._tokens = json.load(json_file)['tokens']
                self._token_number = len(self._tokens)
                self._logger.info('Token loading success. ' +
                                  str(self._token_number) + ' tokens in total.')
        except:
            self._logger.error('Token loading failed!')
            return
        self._data_source = data_sources
        self._token_index = TOKEN_INDEX
        self._lock = threading.Lock()
        self._initialization()
        self._logger.info('Tushare initialized.')

    def _initialization(self):
        '''Create pro_api according the the number of tokens.'''
        self._pros = list()
        for token in self._tokens:
            try:
                # Set token
                ts.set_token(token)
                # Initialize pro api
                pro = ts.pro_api()
                self._pros.append(pro)
            except:
                pro = ts.pro_api(token)
                self._pros.append(pro)

    def switch_token_index(self):
        if self._token_index == (len(self._pros) - 1):
            self._token_index = 0
        else:
            self._token_index += 1

    def get_news(self, pro, news_source, start_time, end_time):
        '''Get news.

        With specific token and news source within the duration.
        Return type is DataFrame.
        '''
        start_time_count = time.time()
        if news_source != 'ctv':
            try:
                df = pro.news(src=DATA_SOURCES[news_source][0],
                              start_date=start_time, end_date=end_time)
                self._logger.info('Get data success with token%s: %s - %s - %s- time used: %ss.' %
                                  (self._token_index, news_source, start_time, end_time, time.time() - start_time_count))
                return df
            except:
                self._logger.error('Get data failed with token%s: %s - %s - %s.' %
                                   (self._token_index, news_source, start_time, end_time))
        else:
            try:
                df = pro.cctv_news(date=start_time)
                self._logger.info('Get data success with token%s: %s - %s - %s - time used: %ss.' %
                                  (self._token_index, news_source, start_time, end_time, time.time() - start_time_count))
                return df
            except:
                self._logger.error('Get data failed with token%s: %s - %s - %s.' %
                                   (self._token_index, news_source, start_time, end_time))

    def get_news_row(self, pro, news_source, start_time, end_time):
        '''Get news.

        With specific token and news source within the duration.
        Return type is a list of rows in DataFrame.
        '''
        data_list = list()
        data = self.get_news(pro, news_source, start_time, end_time)
        if data is None:
            self._logger.error('Get news by row failed!')
            return None
        for _, row in data.iterrows():
            data_list.append(row)
        self._logger.info('Convert DataFrame into rows.')
        return data_list

    def get_long_news(self, news_source, start_time, end_time):
        '''Convert data time into single days before calling get_news(). Return a list of rows.'''
        data_periods = iterate_date(start_time, end_time)
        data_list = list()
        for date_pair in data_periods:
            rows = self.get_news_row(
                self._pros[self._token_index], news_source, date_pair[0], date_pair[1])
            try:
                data_list += rows
            except:
                with open('/home/ubuntu/Desktop/tommy/PyFina/news_update/date.json', 'r+') as json_file:
                    try:
                        date_info = json.load(json_file)
                    except:
                        self._logger.error('Cannot open date.json correctly!')
                        return
                    if date_pair[0] not in date_info['lost_dates'][news_source]:
                        date_info['lost_dates'][news_source].append(
                            date_pair[0])
                        json_file.seek(0)  # Go the beginning of the file
                        json_file.truncate()  # Clear the file before updating it
                        json_file.write(str(json.dumps(date_info)))
            self.switch_token_index()
            time.sleep(SLEEP_INTERVAL)
        self._logger.info('Get news for a long time finished.')
        return data_list

    def get_news_multi(self, news_source, start_time, end_time, pipeline):
        '''Get news with multiprocess.'''
        pipeline.finished_transfer = False
        self._logger.info('Get news multi started!')
        data_periods = iterate_date(start_time, end_time)
        for date_pair in data_periods:
            rows = self.get_news_row(
                self._pros[self._token_index], news_source, date_pair[0], date_pair[1])
            if rows is not None:
                with self._lock:
                    self._logger.warning('Tushare locked.')
                    for item in rows:
                        pipeline.add_item(item, news_source)
                self._logger.warning('Tushare unlocked.')
            else:
                error_token = self._token_index
                self.switch_token_index()
                while self._token_index != error_token:
                    time.sleep(ERROR_INTERVAL)
                    self._logger.warning(
                        'Switch to token %s and try again.' % self._token_index)
                    rows = self.get_news_row(
                        self._pros[self._token_index], news_source, date_pair[0], date_pair[1])
                    try:
                        with self._lock:
                            self._logger.warning('Tushare locked.')
                            for item in rows:
                                pipeline.add_item(item)
                        self._logger.warning('Tushare unlocked.')
                        self._logger.warning("Success after switch!")
                        break
                    except:
                        self.switch_token_index()
                        continue
                if self._token_index == error_token:
                    with open('/home/ubuntu/Desktop/tommy/PyFina/news_update/date.json', 'r+') as json_file:
                        try:
                            date_info = json.load(json_file)
                        except:
                            self._logger.error(
                                'Cannot open date.json correctly!')
                            return
                        if date_pair[0] not in date_info['lost_dates'][news_source]:
                            date_info['lost_dates'][news_source].append(
                                date_pair[0])
                            json_file.seek(0)  # Go the beginning of the file
                            json_file.truncate()  # Clear the file before updating it
                            json_file.write(str(json.dumps(date_info)))
            self.switch_token_index()
            time.sleep(SLEEP_INTERVAL)
        self._logger.info('Get news multi finished.')
        pipeline.finished_transfer = True

    def find_lost_news(self, news_source, pipeline):
        '''The function is intended to find the news on the lost date stored in date.json.'''
        pipeline.finished_transfer = False
        date_info = {}
        with open('/home/ubuntu/Desktop/tommy/PyFina/news_update/date.json', 'r+') as json_file:
            try:
                date_info = json.load(json_file)
            except:
                self._logger.error('Cannot open date.json correctly!')
                return
        try:
            date_list = date_info['lost_dates'][news_source]
        except:
            self._logger("Find lost news error. Cannot load date.json!")
            return
        list_index = 0
        while list_index != len(date_list):
            time.sleep(SLEEP_INTERVAL)
            current_date = date_list[list_index]
            further_date = (datetime.datetime.strptime(
                current_date, STRING_FORMAT) + datetime.timedelta(days=1)).strftime(STRING_FORMAT)
            rows = self.get_news_row(
                self._pros[self._token_index], news_source, current_date, further_date)
            self.switch_token_index()
            if rows is not None:
                with self._lock:
                    self._logger.warning('Tushare locked.')
                    for item in rows:
                        pipeline.add_item(item, news_source)
                self._logger.warning('Tushare unlocked.')
                date_list.pop(list_index)
                with open('/home/ubuntu/Desktop/tommy/PyFina/news_update/date.json', 'r+') as json_file:
                    date_info['lost_dates'][news_source] = date_list
                    json_file.seek(0)
                    json_file.truncate()
                    json_file.write(str(json.dumps(date_info)))
                continue
            else:
                self._logger.warning("Go on to get the next one.")
                list_index += 1
        pipeline.finished_transfer = True

    def find_earliest_date(self, news_source):
        '''Find the earliest time when the data is available.'''
        day_index = 100
        start_time_count = time.time()
        with open('/home/ubuntu/Desktop/tommy/PyFina/news_update/date.json', 'r+') as json_file:
            try:
                date_info = json.load(json_file)
            except:
                self._logger.error('Cannot open date.json correctly!')
                return
            current_date = date_info['start_date'][news_source]
            futher_date = (datetime.datetime.strptime(
                current_date, STRING_FORMAT) + datetime.timedelta(days=1)).strftime(STRING_FORMAT)
            if self.get_news(self._pros[self._token_index], news_source, current_date, futher_date) is not None and\
                    len(self.get_news(self._pros[self._token_index], news_source, current_date, futher_date).head()) != 0:
                self._logger.info('Already up to date.')
                return current_date
            while self.get_news(self._pros[self._token_index], news_source, current_date, futher_date) is None or\
                    (self.get_news(self._pros[self._token_index], news_source, current_date, futher_date) is not None and
                     len(self.get_news(self._pros[self._token_index], news_source, current_date, futher_date).head()) == 0):
                current_date = (datetime.datetime.strptime(
                    current_date, STRING_FORMAT) + datetime.timedelta(days=day_index)).strftime(STRING_FORMAT)
                futher_date = (datetime.datetime.strptime(
                    current_date, STRING_FORMAT) + datetime.timedelta(days=1)).strftime(STRING_FORMAT)
                self.switch_token_index()
                time.sleep(SLEEP_INTERVAL)
            day_index = 10
            while self.get_news(self._pros[self._token_index], news_source, current_date, futher_date) is None or\
                    (self.get_news(self._pros[self._token_index], news_source, current_date, futher_date) is not None and
                     len(self.get_news(self._pros[self._token_index], news_source, current_date, futher_date).head()) == 0):
                current_date = (datetime.datetime.strptime(
                    current_date, STRING_FORMAT) + datetime.timedelta(days=(-day_index))).strftime(STRING_FORMAT)
                futher_date = (datetime.datetime.strptime(
                    current_date, STRING_FORMAT) + datetime.timedelta(days=1)).strftime(STRING_FORMAT)
                self.switch_token_index()
                time.sleep(SLEEP_INTERVAL)
            day_index = 1
            while len(self.get_news(self._pros[self._token_index], news_source, current_date, futher_date).head()) == 0:
                current_date = (datetime.datetime.strptime(
                    current_date, STRING_FORMAT) + datetime.timedelta(days=day_index)).strftime(STRING_FORMAT)
                futher_date = (datetime.datetime.strptime(
                    current_date, STRING_FORMAT) + datetime.timedelta(days=1)).strftime(STRING_FORMAT)
                self.switch_token_index()
                time.sleep(SLEEP_INTERVAL)
            date_info['start_date'][news_source] = current_date
            json_file.seek(0)  # Go the beginning of the file
            json_file.truncate()  # Clear the file before updating it
            json_file.write(str(json.dumps(date_info)))
            self._logger.info('Found the earliest date of %s, the date is %s, time used: %ss' % (
                news_source, current_date, time.time() - start_time_count))
            return current_date


if __name__ == '__main__':
    pass
