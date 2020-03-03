from tornado.web import Application, RequestHandler, HTTPError
from tornado.ioloop import IOLoop
import json

from marshmallow import Schema, fields

import psycopg2
from psycopg2 import sql
from psycopg2.extras import DictCursor

from local_settings import DB_NAME, DB_USER, DB_PASSWORD, DB_HOST

from contextlib import closing

charts = []


class ChartSchema(Schema):
    id = fields.Integer()
    tags = fields.List(fields.String())


class ChartsClient(object):
    def __init__(self):
        self.connect = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                                        password=DB_PASSWORD, host=DB_HOST)
        # raise connect == None
        
    def get_charts(self, id, **kwargs) -> list():
        tag = kwargs.get('tag')
        
        with closing(self.connect) as conn:
            with conn.cursor(cursor_factory=DictCursor) as cursor:
                sql = "SELECT * FROM charts"
                
                if id:
                    cursor.execute(sql + " WHERE id=%s", (id, ))
                    return ChartSchema().dumps(cursor, many=True)

                if tag:
                    sql += " WHERE charts.tags @> ARRAY[%s]"
                    
                cursor.execute(sql, (tag,))
                return ChartSchema().dumps(cursor, many=True)
    

class TodoChart(RequestHandler):
    # def initialize(self, database):
    #     self.database = database
    def get(self, id):
        params = {}
        if 'tag' in self.request.arguments.keys():
            params['tag'] = self.request.arguments.get('tag')[0].decode('utf8')
 
        self.write({'results': ChartsClient().get_charts(id, **params)})
        
    def post(self, _):
        items.append(json.loads(self.request.body))
        self.write({'message': 'new chart added'})
        # raise HTTPError(405)
    
    def delete(self, id):
        global items
        new_items = [item for item in items if item['id'] is not int(id)]
        items = new_items
        self.write({'message': 'Item with id %s was deleted' % id})


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
