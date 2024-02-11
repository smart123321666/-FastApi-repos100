import os
from datetime import date

import psycopg2
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query

app = FastAPI()
load_dotenv()


try:
    connection = psycopg2.connect(
        host=os.getenv('DB_HOST'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        database=os.getenv('DB_NAME')
    )
    cursor = connection.cursor()
except Exception as ex:
    print('[INFO] Error while working with PgSQl', ex)


@app.get('/api/repos/top100')
def get_top_100(sort_by: str = Query("stars"),
                order: str = Query("desc")):
    try:
        sql_query = (f'''SELECT * FROM repositories
                      ORDER BY {sort_by} {order} LIMIT 100''')
        cursor.execute(sql_query)
        result = cursor.fetchall()
        transformed_result = []
        for row in result:
            transformed_result.append({
                'repo': row[1],
                'owner': row[2],
                "position_cur": row[3],
                "position_prev": row[4],
                'stars': row[5],
                'watchers': row[6],
                'forks': row[7],
                'open_issues': row[8],
                'language': row[9]
            })
        return transformed_result
    except Exception as ex:
        raise HTTPException(status_code=500, detail=ex)


@app.get("/api/repos/{owner}/{repo}/activity", response_model=list[dict])
async def get_repository_activity(owner: str,
                                  repo: str,
                                  since: date,
                                  until: date):
    try:
        sql_query = '''
        SELECT "date", commits, authors FROM commits
        JOIN repositories ON commits.repo_id = repositories.id
        WHERE owner = %s AND repo = %s
        AND "date" BETWEEN %s AND %s;
        '''
        cursor.execute(sql_query, (owner, repo, since, until))
        result = cursor.fetchall()
        transformed_result = []
        for row in result:
            transformed_result.append({
                'date': row[0],
                'commits': row[1],
                'authors': row[2]
            })
        return transformed_result
    except Exception as ex:
        raise HTTPException(status_code=500, detail=ex)
