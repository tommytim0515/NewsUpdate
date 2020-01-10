import sys
import logging
import math
import json
from datetime import date, datetime, timedelta
import concurrent.futures
import numpy as np
import mongodb
import tushare_get
import pipeline
from functions import STRING_FORMAT, UPDATE_MAP


if __name__ == '__main__':
    news_source = sys.argv[1] #  Note that the first argv is the name of the python script.
    choose_mode = sys.argv[2]
    # news_source = input("News source:")
    # choose_mode = input("Choose a mode(g/f/u):")
    print('Prepare to run %s - %s' % (choose_mode, news_source))
    logging.basicConfig(level=logging.WARNING)
    mongo = mongodb.MongoDB()
    pipeline = pipeline.Pipeline()
    if choose_mode != 'u':
        tushare = tushare_get.Tushare()
        date_info = {}
        with open('/home/ubuntu/Desktop/tommy/PyFina/news_update/date.json', 'r+') as json_file:
            date_info = json.load(json_file)
        earlist_date = (datetime.strptime(date_info['check_out_date'][news_source], STRING_FORMAT)
                        - timedelta(days=1)).strftime(STRING_FORMAT)
        current_date = date.today().strftime(STRING_FORMAT)

        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
            if choose_mode == 'g':
                get_news = executor.submit(tushare.get_news_multi, news_source, earlist_date,
                                        current_date, pipeline)
            else:
                recover_news = executor.submit(
                    tushare.find_lost_news, news_source, pipeline)
            insert_document = [executor.submit(
                mongo.insert_list_multi, pipeline) for _ in range(5)]

        date_info['check_out_date'][news_source] = current_date
        with open('/home/ubuntu/Desktop/tommy/PyFina/news_update/date.json', 'w') as json_file:
            json_file.write(str(json.dumps(date_info)))
    else:
        source_mongo = mongodb.MongoDB(False, 'CN_Macro', UPDATE_MAP[news_source])
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            update_news = executor.submit(source_mongo.update_from_previous, pipeline, news_source, {})
            insert_document = [executor.submit(
                mongo.insert_list_multi, pipeline) for _ in range(3)]
    print('Finished.')
 
