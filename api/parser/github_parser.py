import os
import psycopg2
import requests
from dotenv import load_dotenv
import schedule
import time

load_dotenv()

URL_API = os.getenv('URL_API')


def get_data():
    response = requests.get(URL_API)
    data = response.json()
    return data


def save_data(data):
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            database=os.getenv('DB_NAME'),
        )
        cur = conn.cursor()

        for item in data:
            cur.execute(
                '''INSERT INTO repositories (repo, owner,
                position_cur, position_prev, stars, watchers,
                forks, open_issues, language) VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s, %s)''',
                (item['repo'], item['owner'],
                 item['position_cur'], item['position_prev'],
                 item['stars'], item['watchers'],
                 item['forks'], item['open_issues'], item['language'])
            )
            repo_id = cur.lastrowid
            cur.execute(
                '''INSERT INTO commits (repo_id, date, commits, authors)
                VALUES (%s, %s, %s, %s)''',
                (repo_id, item['date'], item['commits'], item['authors'])
            )

        conn.commit()
    except (Exception, psycopg2.Error) as error:
        print("Error while working with PostgreSQL", error)
    finally:
        if conn:
            cur.close()
            conn.close()


def job():
    data = get_data()
    save_data(data)


schedule.every().hour.do(job)

while True:
    schedule.run_pending()
    time.sleep(1)
