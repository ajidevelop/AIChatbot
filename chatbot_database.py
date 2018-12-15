__author__ = 'DanielAjisafe'

import pymysql
import json
from datetime import datetime


timeframe = '2007-10'
sql_transaction = []
cleanup = 1000000

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


def create_table():
    cursor.execute('CREATE TABLE IF NOT EXISTS parent_reply('
                   'parent_id VARCHAR(512) PRIMARY KEY, '
                   'comment_id VARCHAR(512) UNIQUE, '
                   'parent TEXT, '
                   'coment TEXT, '
                   'subreddit TEXT, '
                   'unix INT, '
                   'score INT)')


def format_data(data):
    data = data.replace('\n', ' newlinechar ').replace('\r', ' newlinechar ').replace('"', "'")
    return data


def find_parent(pid):
    try:
        sql = "SELECT coment FROM parent_reply WHERE comment_id=%s LIMIT 1"
        cursor.execute(sql, pid)
        result = cursor.fetchone()

        if result is not None:
            return result['coment']
        else:
            return False
    except Exception as e:
        # print('find_parent', e)
        return False


def find_existing_score(pid):
    try:
        sql = "SELECT score FROM parent_reply WHERE parent_id=%s LIMIT 1"
        cursor.execute(sql, pid)
        result = cursor.fetchone()

        if result is not None:
            return result['score']
        else:
            return False
    except Exception as e:
        print('find_parent', e)
        return False


def acceptable(data):
    if len(data.split(' ')) > 50 or len(data) < 1:
        return False
    elif len(data) > 1000:
        return False
    elif data == '[deleted]' or data == '[removed]':
        return False
    else:
        return True


def sql_insert_replace_comment(cid, pid, parent, comment, subreddit, time, score):
    try:
        sql = 'UPDATE parent_reply SET parent_id=%s, comment_id=%s, parent=%s, coment=%s, subreddit=%s, unix=%s, score=%s WHERE ' \
              'parent_id=%s'
        transaction_bldr(sql, (pid, cid, parent, comment, subreddit, int(time), score, pid))
    except Exception as e:
        print('s-UPDATE insertion', str(e))


def sql_insert_has_parent(cid, pid, parent, comment, subreddit, time, score):
    try:
        sql = 'INSERT INTO `parent_reply` (`parent_id`, `comment_id`, `parent`, `coment`, `subreddit`, `unix`, `score`) VALUES (%s, %s, %s, %s, ' \
              '%s, %s, %s)'
        transaction_bldr(sql, (pid, cid, parent, comment, subreddit, int(time), score))
    except Exception as e:
        print('s-PARENT insertion', str(e))


def sql_insert_no_parent(cid, pid, comment, subreddit, time, score):
    try:
        sql = 'INSERT INTO `parent_reply` (`parent_id`, `comment_id`, `coment`, `subreddit`, `unix`, `score`) VALUES (%s, %s, %s, %s, %s, %s)'
        transaction_bldr(sql, (pid, cid, comment, subreddit, int(time), score))
    except Exception as e:
        print('s-NO_PARENT insertion', str(e))


def transaction_bldr(sql, par=False):
    global sql_transaction
    sql_transaction.append([sql, par])
    if len(sql_transaction) > 1000:
        cursor.execute('START TRANSACTION')
        connection.begin()
        for s, p in sql_transaction:
            try:
                if p is False:
                    cursor.execute(s)
                else:
                    cursor.execute(s, p)
            except:
                pass
        connection.commit()
        sql_transaction = []


if __name__ == '__main__':
    create_table()
    row_counter = 0
    paired_rows = 0 + 154172

    with open(f'/Volumes/Mass 1/reddit_data/{timeframe.split("-")[0]}/RC_{timeframe}.out', buffering=1000) as f:
        for row in f:
            # if row_counter < 14800000:
            #     if row_counter % 100000 == 0:
            #         print(f'Total rows read: {row_counter}')
            #     row_counter += 1
            #     continue

            row_counter += 1
            row = json.loads(row)
            parent_id = row['parent_id']
            comment_id = row['link_id']
            body = format_data(row['body'])
            created_utc = row['created_utc']
            score = row['score']
            subreddit = row['subreddit']
            parent_data = find_parent(parent_id)

            if score >= 2:
                if acceptable(body):
                    existing_comment_score = find_existing_score(parent_id)
                    if existing_comment_score:
                        if score > existing_comment_score:
                            sql_insert_replace_comment(comment_id, parent_id, parent_data, body, subreddit, created_utc, score)
                    else:
                        if parent_data:
                            sql_insert_has_parent(comment_id, parent_id, parent_data, body, subreddit, created_utc, score)
                            paired_rows += 1
                        else:
                            sql_insert_no_parent(comment_id, parent_id, body, subreddit, created_utc, score)

            if row_counter % 100000 == 0:
                print(f'Total rows read: {row_counter}, Paired rows: {paired_rows}, Time: {datetime.now()}')

            if row_counter % cleanup == 0:
                print('Cleaning up')
                sql = 'DELETE FROM parent_reply WHERE parent IS NULL'
                cursor.execute(sql)
                connection.commit()

