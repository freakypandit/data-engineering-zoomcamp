FROM python:3.9.1 

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2

WORKDIR /app
COPY zone_ingest.py zone_ingest.py

ENTRYPOINT [ "python", "zone_ingest.py" ]