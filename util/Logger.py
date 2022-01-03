import datetime
import os
import sys


def on_invoke(target=None):
    if target is not None:
        return f'Invocation succeeded. target: {target}. datetime: {datetime.datetime.now()}'
    else:
        return f'Invocation succeeded. datetime: {datetime.datetime.now()}'


def on_write(target=None):
    if target is not None:
        return f'Write succeeded. target: {target}. datetime: {datetime.datetime.now()}'
    else:
        return f'Write succeeded. datetime: {datetime.datetime.now()}'


def on_read(target=None):
    if target is not None:
        return f'Read succeeded. target: {target}. datetime: {datetime.datetime.now()}'
    else:
        return f'Read succeeded. datetime: {datetime.datetime.now()}'


def write_log(location=sys.path[0], message=None, filename='debug_log.txt'):
    file_path = location + os.sep + filename
    _now = str(datetime.datetime.now())
    if os.path.isfile(file_path):
        if message is not None:
            with open(file_path, 'a') as file:
                file.write(_now + ' | ' + message + '\n')
    else:
        if message is not None:
            with open(file_path, 'w') as file:
                file.write(_now + ' | ' + message + '\n')
