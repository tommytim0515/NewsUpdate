import numpy as np
import pandas as pd
from pandas import DataFrame
import mongodb

if __name__ == '__main__':
    mongo1 = mongodb.MongoDB(False)
    mongo2 = mongodb.MongoDB(False, '10JQKA_Fin_News')
    df1_original_frame = DataFrame(list(mongo1.col.find()))
    df1 = df1_original_frame[df1_original_frame['source'] == '同花顺'][['datetime', 'content', 'title']]
    df2  = DataFrame(list(mongo2.col.find()))[['datetime', 'content', 'title']]
    df1 = df1.drop_duplicates(subset=['datetime', 'content', 'title'])
    df2 = df2.drop_duplicates(subset=['datetime', 'content', 'title'])
    df1.index = df1[['datetime', 'content', 'title']]
    df2.index = df2[['datetime', 'content', 'title']]
    # print(df1)
    df_merge = pd.concat([df1, df2], axis=1, sort=True)
    df_merge.index = np.arange(0, len(df_merge))
    a = df_merge[(df_merge.iloc[:,0].isna() == True) & (df_merge.iloc[:,-3].isna() == False)]
    b = df_merge[(df_merge.iloc[:,0].isna() == False) & (df_merge.iloc[:,-3].isna() == True)]

    print(a)
    print(b)

