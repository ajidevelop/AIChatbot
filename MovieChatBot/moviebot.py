__author__ = 'DanielAjisafe'

import pymysql
from tqdm import tqdm, trange

connection = pymysql.connect(
    host='localhost',
    port=3306,
    user='root',
    password='0luwat0b1l0ba',
    db='movie-bot',
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor,
    autocommit=True
)

cursor = connection.cursor()


def create_table():
    cursor.execute('CREATE TABLE IF NOT EXISTS state_reply(statementId INT(6) PRIMARY KEY, replyId INT(6), statement TEXT, reply TEXT, '
                   'movieId INT(3), characterID INT(4))')

    # cursor.execute('CREATE TABLE IF NOT EXISTS movie')




if __name__ == '__main__':
    create_table()
    row_counter = 0
    repliesList = []
    first_replies = []
    list_len = []
    replyNumber = 0

    with open('/Users/danielajisafe/AIChatbot/MovieChatBot/cornell movie-dialogs corpus/movie_conversations.txt', buffering=1000) as f:
        for row in f:
            row = row.split(' +++$+++ ')
            rL = row[-1][2:-3]
            repliesList.append(rL.split("', '"))

    for replies in repliesList:
        first_replies.append(replies[0])
        list_len.append(len(replies))

    reply = pymysql.NULL
    reply_id = 0

    with open('/Users/danielajisafe/AIChatbot/MovieChatBot/cornell movie-dialogs corpus/movie_lines.txt', buffering=1000,
              encoding='iso-8859-1') as f:

        with tqdm(total=304713) as pbar:
            for row in f:
                row = row.split(' +++$+++ ')
                statement_id = int(row[0][1:])
                statement = str(row[-1]).replace('<u>', '').replace('</u>', '')
                movieId = int(row[2][1:])
                characterID = int(row[1][1:])

                sql = 'INSERT INTO `movie-bot`.state_reply (statementId, replyId, statement, reply, movieId, characterID) VALUES ' \
                      '(%s, %s, %s, %s, %s, %s)'
                cursor.execute(sql, (statement_id, reply_id, statement, reply, movieId, characterID))
                connection.commit()

                if reply_id not in first_replies:
                    reply_id = statement_id
                    reply = statement
                else:
                    reply_id = 0
                    reply = pymysql.NULL

                pbar.update(1)

