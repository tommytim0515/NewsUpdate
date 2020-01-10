import queue
import threading
import logging
import logging.handlers
from functions import date_time_formatter, process_content
from functions import CTV_FORMAT, OTHER_FORMAT, DATE_TIME_FORMAT, DATE_FORMAT


MAX_BUFFER_SIZE = 50000000
DOCUMENT_FORMAT = {
    'sin': {'datetime': OTHER_FORMAT, 'content': 1, 'title': 2},
    'ton': {'datetime': OTHER_FORMAT, 'content': 1, 'title': 2},
    'yun': {'datetime': OTHER_FORMAT, 'content': 1, 'title': 2},
    'est': {'datetime': OTHER_FORMAT, 'content': 1, 'title': 2},
    'ctv': {'datetime': CTV_FORMAT, 'content': 2, 'title': 1},
}


class Pipeline(queue.Queue):
    def __init__(self, max_size=MAX_BUFFER_SIZE, document_format=DOCUMENT_FORMAT):
        '''Initialization of Pipeline, the sub-class of Queue.'''
        super().__init__(maxsize=max_size)
        self._document_format = document_format
        self._logger = logging.getLogger(__name__)
        file_handler = logging.handlers.RotatingFileHandler(
            '/home/ubuntu/Desktop/tommy/PyFina/news_update/logs/pipeline.log', maxBytes=1024*1024, backupCount=3)
        file_handler.setLevel(logging.INFO)
        file_format = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_format)
        self._logger.addHandler(file_handler)
        self.finished_transfer = False
        self._logger.info('Pipeline initialized.')

    def add_item(self, items, news_source):
        '''Add item in proper format and show the debug message.'''
        try:
            dict = {}
            item_list = items.values.tolist()
            for i in range(len(item_list)):
                if item_list[i] is None or type(item_list[i]) != str:
                    item_list[i] = ''
            if news_source == 'sin':
                if item_list[2] == '':
                    item_list[1], item_list[2] = process_content(item_list[1])
                else:
                    item_list[1], _ = process_content(item_list[1])
            for key, index in self._document_format[news_source].items():
                if key == 'datetime':
                    dict.update({'datetime': date_time_formatter(
                        item_list[0], DATE_TIME_FORMAT, news_source)})
                    dict.update({"date": date_time_formatter(
                        item_list[0], DATE_FORMAT, news_source)})
                else:
                    dict.update({key: item_list[index].strip()})
            if news_source == 'sin':
                dict.update({'source': '新浪财经'})
            elif news_source == 'ton':
                dict.update({'source': '同花顺'})
            elif news_source == 'yun':
                dict.update({'source': '云财经'})
            elif news_source == 'est':
                dict.update({'source': '东方财富'})
            elif news_source == 'ctv':
                dict.update({'source': 'CCTV'})
            self.put(dict)
            self._logger.info(
                "Adding item success! Pipeline size %s." % self.qsize())
        except:
            self._logger.error('Adding item failed! %s' % item_list)
            
    def add_item_update(self, item_dict, news_source):
        '''Add item in proper format and show the debug message when updating from previous data.'''
        try:
            dict = {}
            for key in item_dict:
                if item_dict[key] is None or type(item_dict[key]) != str:
                    item_dict[key] = ''
            if news_source != 'ctv':
                dict.update({'datetime': item_dict['datetime']})
                dict.update({'date': date_time_formatter(item_dict['datetime'], DATE_FORMAT, news_source)})
            else:
                dict.update({'datetime': date_time_formatter(item_dict['date'], DATE_TIME_FORMAT, news_source)})
                dict.update({'date': item_dict['date']})
            dict.update({'content': item_dict['content']})
            dict.update({'title': item_dict['title']})
            if news_source == 'ton':
                dict.update({'source': '同花顺'})
            elif news_source == 'yun':
                dict.update({'source': '云财经'})
            elif news_source == 'est':
                dict.update({'source': '东方财富'})
            elif news_source == 'ctv':
                dict.update({'source': 'CCTV'})
            self.put(dict)
            self._logger.info(
                "Adding item success! Pipeline size %s." % self.qsize())
        except:
            self._logger.error('Adding item failed! %s' % item_dict)

    def get_item(self):
        '''Get the first item stored in the queue.'''
        try:
            result = self.get()
            self._logger.info(
                'Getting item success! Pipeline size %s.' % self.qsize())
            return result
        except:
            self._logger.error('Getting item failed!')


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    pass
