__author__ = 'DanielAjisafe'

import pymysql
import pandas as pd

timeframes = ['2015-05']

for timeframe in timeframes:
    connection = pymysql.connect(
        host='localhost',
        port=3306,
        user='root',
        password='0luwat0b1l0ba',
        db=f'{timeframe}',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True
    )
    cursor = connection.cursor()
    limit = 5000
    last_unix = 0
    cur_length = limit
    counter = 0
    test_done = False
    while cur_length == limit:
        df = pd.read_sql('SELECT * FROM parent_reply WHERE unix > {} AND parent IS NOT NULL AND score > 0 ORDER BY unix ASC LIMIT {}'.format(last_unix, limit), connection)
        last_unix = df.tail(1)['unix'].values[0]
        cur_length = len(df)
        if not test_done:
            with open('test.from', 'a', encoding='utf8') as f:
                for content in df['parent'].values:
                    f.write(content+'\n')
            with open('test.to', 'a', encoding='utf8') as f:
                for content in df['coment'].values:
                    f.write(content+'\n')
            test_done = True
        else:
            with open('train.from', 'a', encoding='utf8') as f:
                for content in df['parent'].values:
                    f.write(content+'\n')
            with open('train.to', 'a', encoding='utf8') as f:
                for content in df['coment'].values:
                    f.write(content+'\n')

        counter += 1
        if counter % 20 == 0:
            print(counter*limit, 'rows completed so far')
