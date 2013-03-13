#!/usr/bin/env python
# -*- coding: utf-8 -*-
import psycopg2
from struct import Struct
from time import time

raw_data = [
    'eNrt0TENACAAA8GOTAQdqEQKAhCJA8JIwp2C5pukJn0FAACAk/nmrOKZ7zQJgIcMCQAAAAAAuLYBWvQCHQ==',
    'eNrt0FERABAAQLEXwAkipUBCauF8bBFWzVonAAAAAOCBoQCAj2wFAAAAwDMXioMBcQ==',
    'eNrt0DERgDAURMFj6CgQQoWESEHKF4RI2ggIpGBXwN3MS7Inxx1esw1ZKSEBAAB6pwQAdJbSAAAARlg/f2yiA/BDlwQzlQQTPQXRA0M=',
    'eNrt2bEJwCAARcEflJTiHM7ukLa2gYAIdyO89iVpyZgBAACqBAAAnzwSAFyqSHCNLgEAwO6VAOAvR/74AgjYAQc=',
    'eNrt0DENACAQALFL2BHy2hGJBdhbCa12zQkAAAAAAAAAAACAH0vBuwsy6wDo',
    'eNrt2CEBACEABMGjABnQn4ZIBCIkFvsSmImw4sQlqck3AwAA5ygSXKRLgD0CADjHkACAtzUJfvOeAFhTgM0CfdoB2Q==',
    'eNrt0sEJgDAQBMBVfAaxjrwsIaWlIIu0Ax+CEMlMBXu7l2RP6hWaCgAAgE+dP8u7dJsB8NqqAgAY0KECAOBJm+HIbYopi28GhnUDcbYCOA==',
    'eNrt0bENgDAUQ0GHpEQMkikzEEMyQRpE88XdBNZzkiuZd6CeQwIAgF9pEnxpSLB1SgAAUFOvPX95EAAAAGpotr33ABMZAX0=',
    'eNrtyjEBACAQAKFLYBCzfwijWUF3mKlW7QkAAAAAAAAAAAAA/pzXeAFOJAGl',
    'eNrt0AENACAQAKGbAQzy2Q1pDecgAtWuOcGHlgIAAAAAAAAAAAB4wgV/DADn',
]

def create_table(cursor):
    cursor.execute('CREATE TABLE hll_test (data bytea)')
    for r in raw_data:
        cursor.execute('INSERT INTO hll_test (data) VALUES(%s)', (psycopg2.Binary(r.decode('base64')),))

def create_big_table(cursor):
    cursor.execute('CREATE TABLE hll_big_test (data bytea)')
    for _ in xrange(10000):
        for r in raw_data:
            cursor.execute('INSERT INTO hll_big_test (data) VALUES(%s)', (psycopg2.Binary(r.decode('base64')),))

conn = psycopg2.connect('user=bobrov')
cursor = conn.cursor()
#create_table(cursor)
#create_big_table(cursor)
#conn.commit()

s = time()
#cursor.execute('SELECT hll_count(hll_merge(hll_decode(data))) from hll_big_test')
cursor.execute('SELECT hll_sum(data) from hll_big_test')
print cursor.fetchone()
print time() - s