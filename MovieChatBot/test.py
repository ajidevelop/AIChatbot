__author__ = 'DanielAjisafe'

from tqdm import tqdm

with tqdm(total=100000000) as pbar:
    for i in range(100000000):
        pbar.update(1)