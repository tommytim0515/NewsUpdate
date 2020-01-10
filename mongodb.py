import datetime
import json
import threading
import logging
import logging.handlers
import pandas as pd
from pymongo import MongoClient
import pipeline
from functions import (USER, PASSWORD, HOST, PORT, AUTH_SOURCE,
                       URL_APPENDIX, CONNECTION_STRING, DB_NAME, COL_NAME, TRANSFER_SIZE)


class MongoDB:
    def __init__(self, check=True, collection=COL_NAME, database=DB_NAME, 
                 user=USER, password=PASSWORD, host=HOST,
                 port=PORT, auth_source=AUTH_SOURCE, url_appendix=URL_APPENDIX,
                 connection_string=CONNECTION_STRING):
        '''Initialize the object.

        Specify the name of the database and the connection method.
        if the *argv is empty, it will use the CONNECTION_STRING as the default connection string.
        '''
        # Configuration of logging module
        self._logger = logging.getLogger(__name__)
        file_handler = logging.handlers.RotatingFileHandler(
            '/home/ubuntu/Desktop/tommy/PyFina/news_update/logs/mongodb.log', maxBytes=1024*1024, backupCount=3)
        file_handler.setLevel(logging.INFO)
        file_format = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_format)
        self._logger.addHandler(file_handler)
        self._database = database
        self._collection = collection
        self._connection_string = ''
        self.connected = False  # This is the flag for connection establishment
        if connection_string != '':
            self._connection_string = connection_string
        else:
            self.connection_string = ('mongodb://' + user + ':'
                                      + password + '@'
                                      + host + ':'
                                      + port + '/?authSource='
                                      + auth_source + url_appendix)
        self._connect()
        if check:
            self._check()
        self._lock = threading.Lock()
        self._logger.info('MongoDB initialized.')

    def _connect(self):
        try:
            '''Connect to the server and create or find corresponding database and collection.'''
            self._client = MongoClient(self._connection_string, connect=False)
            # Create or find the database
            self.db = self._client[self._database]
            # Create or find the collection
            self.col = self.db[self._collection]
            self.connected = True
            self.col.find().limit(-100)
        except:
            self._logger.error('MongoDB Connection failed!')

    def _check(self):
        '''Check collection.

        Check the status of the collection after connection.
        Currently it only check the index of the collection.
        '''
        # index_information = self.col.index_information()
        # for key, value in INDEX_CHECK.items():
        #     if key not in index_information:
        #         self.col.create_index([(key, value)])
        #         self._logger.info(
        #             'Index %s created in order %d.' % (key, value))
        self.col.create_index([('datetime', 1)])
        self.col.create_index(
            [('datetime', 1), ('content', 1), ('title', 1)], unique=True)
        self._logger.info('Indexes created.')

    def switch_collection(self, collection=None, database=None):
        '''Switch the collection (including database). Note: "collection" is the first argument'''
        if collection is None:
            return
        if database is not None:
            self._database = database
            self._collection = collection
            self.db = self._client[database]
            self.col = self.db[collection]
            self._logger.info('Collection switched to %s in %s' %
                              (self._collection, self._database))

    def document_count(self, counter_filter={}):
        '''Count the number of the document after filtering. Return type is integer.'''
        document_count = self.col.count_documents(filter=counter_filter)
        return document_count

    def read_collection(self, read_filter={}, skip=0, limit=None):
        '''Fetch data with common method.

        Get the data through the filter within the limitation. Also you can control the starting point with "skip".
        The return type is DataFrame.
        '''
        document_number = self.document_count(read_filter)
        if skip > document_number:
            self._logger.error('Too many to skip, no document to show!')
            return
        if limit is not None and limit < 0:
            self._logger.error('Limitation setting error!')
            return
        if limit is not None:
            data_frame = pd.DataFrame(self.col.find(
                read_filter).skip(skip).limit(limit))
        else:
            data_frame = pd.DataFrame(self.col.find(read_filter).skip(skip))
        return data_frame

    def insert_document(self, data_frame):
        '''Insert document(s) into the collection with common method.

        Argument type is DataFrame. Update time will be logged.
        '''
        self.col.insert_many(json.loads(data_frame.T.to_json()).values())
        self._logger.info('Data Inserted in ' + self._collection +
                          ' in ' + self._database)

    def insert_list(self, items):
        '''Insert a list of rows in DataFrame.'''
        if len(items) < 0:
            self._logger('Empty list! Nothing to insert to Mongodb.')
            return
        # elif len(items) == 1:
        #     self.col.insert_one(items[0])
        # else:
        #     try:
        #         self.col.insert_many(items)
        #     except:
        #         self._logger.error('Insert many error!')
        for item in items:
            try:
                self.col.insert_one(item)
            except:
                self._logger.error('Insertion error! Duplicate?')
        self._logger.info('%s documents inserted.' % len(items))

    def insert_list_multi(self, pipeline):
        '''Insert the item in the pipeline. For concurrency use.'''
        self._logger.info('Insert list multi started.')
        while (not pipeline.finished_transfer) or (not pipeline.empty()):
            if not pipeline.empty():
                self._lock.acquire()
                self._logger.warning('Mongo locked.')
                transfer_list = list()
                for _ in range(TRANSFER_SIZE):
                    if pipeline.empty():
                        continue
                    transfer_list.append(pipeline.get_item())
                self._lock.release()
                self._logger.warning('Mongo unlocked.')
                self.insert_list(transfer_list)
                self._logger.info('%d documents inserted.' %
                                  len(transfer_list))
        self._logger.info("Insert list multi finished.")

    def drop_document(self, drop_filter={}):
        '''Delete the document(s).

        Delete corresponding documents according to the filter, return the number of documents deleted.
        Return type is integer.
        '''
        delete_document = self.col.delete_many(filter)
        delete_count = delete_document.deleted_count
        self._logger.info(str(delete_count) + ' documents deleted.')
        return delete_count
    
    def update_from_previous(self, pipeline, news_source, filter_update={}):
        '''Update the collection with previous collection.'''
        self._logger.info('Update from previous started.')
        for doc in self.col.find(filter_update, {'_id': 0}):
            with self._lock:
                self._logger.warning('Mongo locked.')
                pipeline.add_item_update(doc, news_source)
            self._logger.warning('Mongo unlocked.')
        pipeline.finished_transfer = True
        self._logger.info('Update from previous done.')

    def close_database(self):
        '''Close the database.'''
        self.col.close()


if __name__ == '__main__':
    mongo = MongoDB()
    mongo.col.create_index([('datetime', 1)])
    mongo.col.create_index([('datetime', 1)])
