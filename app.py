from tornado.web import Application, RequestHandler, HTTPError
from tornado.ioloop import IOLoop
import json

from marshmallow import Schema, fields, ValidationError

import psycopg2
from psycopg2 import sql
from psycopg2.extras import DictCursor

from local_settings import DB_NAME, DB_USER, DB_PASSWORD, DB_HOST

from contextlib import closing


class ChartSchema(Schema):
    id = fields.Integer(required=True)
    tags = fields.List(fields.String())


class ChartsClient(object):
    def __init__(self):
        self.connect = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                                        password=DB_PASSWORD, host=DB_HOST)
        # raise connect == None
    
    def update_or_insert_chart(self, idd: int, body: dict, update: bool) -> bool:
        if update:
            update = sql.SQL('UPDATE charts SET tags={} WHERE id={}').format(sql.Literal(body['tags']), sql.Literal(idd))
            self.execute(update, need_fetch=False)
            return True
        
        insert = sql.SQL('INSERT INTO charts (id, tags) VALUES {}').format(
            sql.SQL(',').join(map(sql.Literal, [(body['id'], body['tags'])])
        ))
        self.execute(insert, need_fetch=False)
        return True

    def delete(self, idd: int) -> None:
        delete = sql.SQL('DELETE FROM charts WHERE id={}').format(sql.Literal(idd))
        self.execute(delete, need_fetch=False)

    def execute(self, sql: sql.Composable,  *args, need_fetch=True):
        result = None
        with closing(self.connect) as conn:
            with conn.cursor(cursor_factory=DictCursor) as cursor:
                conn.autocommit = True
                cursor.execute(sql, (args,))
                if need_fetch:
                    result = cursor.fetchall()
        return result
    
    def get_charts(self, idd: int, **kwargs):
        tag = kwargs.get('tag')
        
        get_sql = sql.SQL('SELECT * FROM charts')
        if tag:
            tags = ['charts.tags @> ARRAY[\'%s\']' % item for item in tag.split(',')]

        if idd or tag:  # what?? idd primary key! i don't know =(
            get_sql += sql.SQL(' WHERE ')
            if idd:
                get_sql += sql.SQL('id=%s' % idd) if not tag else sql.SQL(' id=%s and (%s)' % (idd, ' or '.join(tags)))
            else:
                get_sql += sql.SQL(' or '.join(tags))
        return ChartSchema().dumps(self.execute(get_sql), many=True)


class TodoChart(RequestHandler):
    # def initialize(self, database):
    #     self.database = database
    def get(self, idd: int):
        """ get chart or charts """
        params = {}
        if 'tag' in self.request.arguments.keys():
            params['tag'] = self.request.arguments.get('tag')[0].decode('utf8')
        
        self.write({'results': json.loads(ChartsClient().get_charts(idd, **params))})
    
    def post(self, idd: int):
        """ insert or update charts """
        try:
            body = ChartSchema().loads(self.request.body)  # deserializing Objects
        except ValidationError as err:
            raise HTTPError(400, str(err.messages))
        except json.JSONDecodeError:
            raise HTTPError(500, 'json decode error')
        
        if ChartsClient().update_or_insert_chart(idd, body, update=True if idd else False):
            self.write({'message': 'update/insert with id {}: OK'.format(idd)})
        else: raise HTTPError(500)

    def delete(self, idd: int):
        ChartsClient().delete(idd)
        self.write({'message': 'Chart with id %s was deleted' % idd})


def make_app():
    
    urls = [
        # ("/", TodoCharts),
        (r"/api/chart/([^/]+)?", TodoChart)
    ]
    return Application(urls, debug=True)


if __name__ == '__main__':
    app = make_app()
    app.listen(3000)
    IOLoop.instance().start()
