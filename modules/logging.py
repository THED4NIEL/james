from icecream import ic
import _thread as thread
import queue


def warn(warn: str):
    _log_queue.put('W> ' + warn)


def info(info: str):
    _log_queue.put('I> ' + info)


def err(err: str):
    _log_queue.put('E> ' + err)


def show_log():
    while True:
        try:
            log = _log_queue.get(block=True)
            ic(log)
        except queue.Empty:
            continue


def show_log_threaded():
    return thread.start_new_thread(show_log, ())


_log_queue = queue.Queue()
