import psycopg2
from psycopg2 import sql
from psycopg2.extras import DictCursor
from contextlib import closing
from local_settings import DB_NAME, DB_USER, DB_PASSWORD, DB_HOST

from app import ChartSchema

with closing(psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST)) as conn:
    
    with conn.cursor(cursor_factory=DictCursor) as cursor:
        conn.autocommit = True

        # create table
        cursor.execute('CREATE TABLE charts (id SERIAL PRIMARY KEY NOT NULL, tags text ARRAY)')

        values = [
            ('100100', "{tesla, mask, ev}"),
            ('100101', "{moscow, product}"),
            ('100102', "{test, space}"),
            ('100103', "{alone, virtualenv}"),
            ('100104', "{pip, dev, tty}"),
        ]
        insert = sql.SQL('INSERT INTO charts (id, tags) VALUES {}').format(
            sql.SQL(',').join(map(sql.Literal, values))
        )
        # insert test data
        cursor.execute(insert)

        # create index
        cursor.execute('CREATE INDEX ON charts using gin(tags)')

        # disable seqscan
        cursor.execute('SET enable_seqscan TO off')
        
        # get query from charts
        cursor.execute('SELECT * FROM charts')
        chart = ChartSchema().dumps(cursor, many=True)
        print(chart)

