import datetime


STRING_FORMAT = '%Y%m%d'
DATE_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
DATE_FORMAT = '%Y-%m-%d'
CTV_FORMAT = '%Y%m%d'
OTHER_FORMAT = '%Y-%m-%d %H:%M:%S'
OPTIONAL_FORMAT = '%Y-%m-%d %H:%M'

DATA_SOURCES = {
    'sin': ('sina', '新浪财金'),
    'ton': ('10jqka', '同花顺'),
    'yun': ('yuncaijing', '云财经'),
    'est': ('eastmoney', '东方财富')
}
TOKEN_INDEX = 0
SLEEP_INTERVAL = 0.01
ERROR_INTERVAL = 0

USER = ''
PASSWORD = ''
HOST = ''
PORT = ''
AUTH_SOURCE = ''
URL_APPENDIX = ''
CONNECTION_STRING = ''

DB_NAME = ''
COL_NAME = ''


TRANSFER_SIZE = 200

EARLIST_DATE = {
    'sin': '20181008',
    'ton': '20181008',
    'yun': '20181008',
    'est': '20181008',
    'ctv': '20070201'
}

UPDATE_MAP = {
    'ton': '10JQKA_Fin_News',
    'ctv': 'CCTV_Fin_News',
    'est': 'Eastmoney_Fin_News',
    'yun': 'Yuncaijing_Fin_News'
}


def iterate_date(start_date, end_date):
    '''Separate time period into single days.

    The maximum number of pieces of news per request is 1000.
    So we need to divide time period into days in case the request is too big.
    Return a list of dates.
    '''
    date_list = list()
    current_date = start_date
    futher_date = start_date
    while futher_date != end_date:
        current_date = futher_date
        futher_date = (datetime.datetime.strptime(
            futher_date, STRING_FORMAT) + datetime.timedelta(days=1)).strftime(STRING_FORMAT)
        date_list.append((current_date, futher_date))
    return date_list


def date_time_formatter(date_time, date_format, news_source):
    '''Convert date and time into correct form. Return type is string.'''
    if news_source == 'ctv':
        new_format = datetime.datetime.strptime(
            date_time, CTV_FORMAT).strftime(date_format)
        return new_format
    else:
        try:
            new_format = datetime.datetime.strptime(
                date_time, OTHER_FORMAT).strftime(date_format)
        except:
            new_format = datetime.datetime.strptime(
                date_time, OPTIONAL_FORMAT).strftime(date_format)
        return new_format


def process_content(content):
    '''Sort the content.

    Some news got from Sina is not clean enough. This function helps to sort the content and generate title.
    Return content and title.
    '''
    title = ''
    # Leave out the blank spaces, '\n' and '\t'
    content = content.strip()
    if content[0] == '【':
        index = 1
        while index < len(content):
            if content[index] == '】':
                break
            index += 1
        if index < len(content) - 1 and index > 1:
            title = content[1: index]
            content = content[index + 1:]
    return content, title


if __name__ == '__main__':
    pass
