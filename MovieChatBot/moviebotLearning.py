__author__ = 'DanielAjisafe'

import tensorflow as tf
import pandas as pd
from tqdm import tqdm
import MovieChatBot.moviebot as mb


number_of_rows = mb.cursor.execute('SELECT COUNT(*) FROM `movie-bot`.state_reply;')

with tqdm(range(number_of_rows)) as pbar:
    limit = 5000
    current_amount = limit
    while current_amount == limit:
        df = pd.read_sql('SELECT * FROM `movie-bot`.state_reply LIMIT {}'.format(limit), mb.connection)
        current_amount = len(df)

    pbar.update(1)
