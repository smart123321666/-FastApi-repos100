import os

import psycopg2
import requests
from dotenv import load_dotenv
load_dotenv()

DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_DATABASE = os.getenv('DB_NAME')

URL_API = os.getenv('URL_API')

def get_data():
    response = requests.get(URL_API)
    data = response.json()
    return data

def save_data(data):
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_DATABASE,
        )
        cur = conn.cursor()

        for item in data:
            cur.execute(
                'INSERT INTO repositories (repo, owner, position_cur, position_prev, stars, watchers, forks, open_issues, language) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)',
                (item['repo'], item['owner'], item['position_cur'], item['position_prev'], item['stars'], item['watchers'], item['forks'], item['open_issues'], item['language'])
            )
            repo_id = cur.lastrowid
            cur.execute(
                'INSERT INTO commits (repo_id, date, commits, authors) VALUES (%s, %s, %s, %s)',
                (repo_id, item['date'], item['commits'], item['authors'])
            )

        conn.commit()
    except (Exception, psycopg2.Error) as error:
        print("Ошибка при работе с PostgreSQL", error)
    finally:
        if conn:
            cur.close()
            conn.close()
