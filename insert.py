#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import os
import glob
import sys
import shutil
import mysql.connector

from configparser import ConfigParser


def csv_to_mysql(files):
    """
    MySQLへデータを登録する。
    速度が遅いと感じたらmultiple insertも検討する
    """
    config = ConfigParser()
    config.read('/volume1/observer/config.ini')
    conn = mysql.connector.connect(
        host=config.get('mysql', 'host'),
        port=config.getint('mysql', 'port'),
        user=config.get('mysql', 'user'),
        password=config.get('mysql', 'password'),
        database=config.get('mysql', 'database'),
        charset=config.get('mysql', 'charset'),
    )

    cursor = conn.cursor()
    for file in files:
        print(file)
        print(type(file))
        print(file.find('np213i'))
        if file.find('np213i') != -1:
            with open(file, 'r') as csv_file:
                reader = csv.reader((line.replace('\0', '') for line in csv_file))
                for row in reader:
                    try:
                        device_id = int(row[2]) - 1
                        cursor.execute('insert into sensors values (%s, %s, %s, %s, %s, %s)', (row[1], device_id, 0, 0, float(row[7]), row[0]))
                    except Exception as e:
                        print(e)
                        conn.rollback()
        else:
            with open(file, 'r') as csv_file:
                reader = csv.reader((line.replace('\0', '') for line in csv_file))
                print(type(reader))
                for row in reader:
                    print(row)
                    try:
                        cursor.execute('insert into sensors values (%s, %s, %s, %s, %s, %s)', row)
                    except Exception as e:
                        print(e)
                        conn.rollback()
        dst = file.split('/')
        dst.insert(-1, 'inserted')
        dst = '/'.join(dst)
        print(dst)
        shutil.move(file, dst)

    conn.commit()
    cursor.close()
    conn.close()


if __name__ == '__main__':
    csv_to_mysql(glob.glob('/volume1/observer/*/*/*.csv'))
