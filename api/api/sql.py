import os

import psycopg2
import os
from dotenv import load_dotenv


load_dotenv()

try:
    connection = psycopg2.connect(
        host=os.getenv('DB_HOST'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        database=os.getenv('DB_NAME')
    )
    cursor = connection.cursor()
    connection.autocommit = True

    with connection.cursor() as cursor:
        cursor.execute(
            'SELECT version();'
        )
        print(f'Server version:{cursor.fetchone()}')
    with connection.cursor() as cursor:
        cursor.execute(
            '''CREATE TABLE repositories (
            id SERIAL PRIMARY KEY,
            repo VARCHAR(255),
            owner VARCHAR(255),
            position_cur INTEGER,
            position_prev INTEGER,
            stars INTEGER,
            watchers INTEGER,
            forks INTEGER,
            open_issues INTEGER,
            language VARCHAR(50)
            );'''
        )
        print('[INFO] Table Repositories created successfully')

    with connection.cursor() as cursor:
        for i in range(5):
            cursor.execute(
                '''INSERT INTO repositories (repo, owner, position_cur,
                position_prev, stars, watchers, forks, open_issues, language)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)''',
                ('repo_' + str(i),
                 'owner_' + str(i), i, i-1, i*100,
                 i*50, i*30, i*20, 'Python')
            )
        print('[INFO] Data inserted into Repositories table successfully')

    with connection.cursor() as cursor:
        cursor.execute(
            '''CREATE TABLE commits (
                id SERIAL PRIMARY KEY,
                repo_id INTEGER REFERENCES repositories(id),
                date DATE,
                commits INTEGER,
                authors text[]
                );'''
        )
        print('[INFO] Table Commits created successfully')
    with connection.cursor() as cursor:
        for i in range(5):
            cursor.execute(
                '''INSERT INTO commits (repo_id, date, commits, authors)
                VALUES (%s, %s, %s, %s)''',
                (i+1, '2022-01-0'+str(i+1), (i+1)*10, ['author1', 'author2'])
            )
    print('[INFO] Data inserted into Commits table successfully')

except Exception as ex:
    print('[INFO] Error while working with PgSQl', ex)
finally:
    if connection:
        connection.close
        print('[INFO] PostgreSQL connection closed')
