FROM python:latest


COPY requirements.txt requirements.txt

RUN pip install --no-cache-dir --upgrade -r requirements.txt
RUN pip install python-dotenv


COPY ./api api

# CMD ["sh", "-c", "source .env && python api/sql.py && python api/main.py"]
CMD ["sh", "-c", "python -c 'from dotenv import load_dotenv; load_dotenv(); import api.sql; import api.main'"]
