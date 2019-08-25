# -*- coding: utf-8 -*-

import contextlib
from mysql_self import Pool
import random

"""
自己封装的数据库包，mysql_self
"""
host = ""
port = 3306
user = "read"
password = "QMDb9w9dnBGhxgRnq3sd"
database = "db"


def __con(module, r=True):
    pool = Pool.getMySqlInstance(module)
    if r:
        conn = random.choice(pool['r']).connection()
    else:
        conn = pool["w"].connection()
    return conn


@contextlib.contextmanager
def get_cursor(module, r=True):
    conn = __con(module, r)
    cursor = conn.cursor()
    try:
        yield cursor
    except Exception as e:
        conn.rollback()
        raise
    finally:
        conn.commit()
        cursor.close()
        conn.close()


@contextlib.contextmanager
def get_conn(module, r=True):
    conn = __con(module, r)
    try:
        yield conn
    except Exception as e:
        conn.rollback()
        raise
    finally:
        conn.close()
