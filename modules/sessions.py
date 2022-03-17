import datetime
from dbj import dbj
import os
import pickle

from modules.classes import SearchConfig

sessions_base_dir = os.path.join('.', 'sessions')
sessions_default_dir = os.path.join('.', 'sessions', 'current')

if not os.path.exists(sessions_base_dir):
    os.mkdir(sessions_base_dir)
if not os.path.exists(sessions_default_dir):
    os.mkdir(sessions_default_dir)


def continue_session():
    x = os.listdir(sessions_default_dir)
    if x:
        print('(C)ontinue session or create (N)ew one?')
        sel = input('(C) continue, (N) new: ').strip()

        if not sel or sel in ('n', 'N'):
            create_new_session()
            return False
        elif sel in ('c', 'C'):
            return True


def create_new_session():
    x = os.listdir(sessions_default_dir)

    if x:
        created_date, _ = [x for x in os.listdir(
            sessions_default_dir) if '.created' in x]
        os.rename(sessions_default_dir, os.path.join(
            sessions_base_dir, created_date))
        os.mkdir(sessions_default_dir)
    time = str(datetime.datetime.now().strftime('%y%m%d_%H%M'))
    with open(os.path.join(sessions_default_dir, f'{time}.created'), 'w') as f:
        f.write('')
