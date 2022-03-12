import datetime
from dbj import dbj
import os
import pickle

from classes import SearchConfig

sessions_default_dir = os.path.join('.', 'sessions')
if not os.path.exists(sessions_default_dir):
    os.mkdir(sessions_default_dir)
sessiondb = dbj(os.path.join(sessions_default_dir,
                             'sessions.json'), autosave=True)
sessionpath = ''


def select_session():
    if sessions := sessiondb.getallkeys():
        ctr = 0
        print('Select session via number or "0" to create a new session.\n0: NEW SESSION')
        for key in sessions:
            ctr += 1
            print(f'{ctr}: {key}')
        sel = int(input('Selection: ').strip())
        rn = range(ctr)

        if not sel:
            active_session = create_new_session()
        elif 0 < sel <= ctr:
            active_session = os.path.join(
                sessions_default_dir, sessions[int(sel)-1])

        return active_session
    else:
        print('No previous sessions found, creating new one.')
        return create_new_session()


def create_new_session():
    time = str(datetime.datetime.now().strftime('%y%m%d_%H%M'))
    sessionpath = os.path.join(sessions_default_dir, time)
    os.mkdir(sessionpath)
    sessiondb.insert({'sessionpath': sessionpath}, time)
    return sessionpath


def save_search_config(options: SearchConfig):
    with open(os.path.join(sessionpath, 'config.bin'), 'wb') as sessionexport:
        pickle.dump(options, sessionexport)
