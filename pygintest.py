from contextlib import closing

from local_settings import DB_NAME, DB_USER, DB_PASSWORD, DB_HOST

from marshmallow import Schema, fields, ValidationError

import psycopg2
from psycopg2 import sql
from psycopg2.extras import DictCursor, DictRow

from tornado.web import Application, RequestHandler, HTTPError, url
from tornado.ioloop import IOLoop

from typing import List, Any, Union

import json


class ChartSchema(Schema):
    id = fields.Integer(required=True)
    tags = fields.List(fields.String())


class ChartsClient(object):
    def __init__(self):
        self.connect = psycopg2.connect(dbname=DB_NAME, user=DB_USER,
                                        password=DB_PASSWORD, host=DB_HOST)
    
    def update_or_insert_chart(self, idd: int, body: dict, update: bool) -> None:
        if update:
            update = sql.SQL('UPDATE charts SET tags={} WHERE id={}').format(sql.Literal(body['tags']), sql.Literal(idd))
            self.execute(update, need_fetch=False)
            return
        
        insert = sql.SQL('INSERT INTO charts (id, tags) VALUES {}').format(
            sql.SQL(',').join(map(sql.Literal, [(body['id'], body['tags'])])
        ))
        self.execute(insert, need_fetch=False)

    def delete(self, pk: int) -> None:
        delete = sql.SQL('DELETE FROM charts WHERE id={}').format(sql.Literal(pk))
        self.execute(delete, need_fetch=False)

    def execute(self, raw_sql: sql.Composable, *args, need_fetch=True) -> Union[List[Union[DictRow, None]], None]:
        result = None
        with closing(self.connect) as conn:
            with conn.cursor(cursor_factory=DictCursor) as cursor:
                conn.autocommit = True
                cursor.execute(raw_sql, (args,))
                if need_fetch:
                    result = cursor.fetchall()
        return result
    
    def get_charts(self, pk: int, **kwargs: Any) -> List[Union[DictRow, None]]: #str:
        tag: str = kwargs.get('tag')
        
        get_sql = sql.SQL('SELECT * FROM charts')
        
        if pk:
            get_sql += sql.SQL(' WHERE id=%s' % pk)
            return self.execute(get_sql)
        
        if tag:
            tags = ['charts.tags @> ARRAY[\'%s\']' % item for item in tag.split(',')]
            get_sql += sql.SQL(' WHERE (%s)' % ' or '.join(tags))
            
        return self.execute(get_sql)


class InfoProject(RequestHandler):
    def get(self) -> None:
        self.write('<a href="%s">link to charts</a>' %
                   self.reverse_url("chart", ''))


class TodoChart(RequestHandler):

    def get(self, pk: int) -> None:
        """ get chart or charts """
        params = {}
        if 'tag' in self.request.arguments.keys():
            params['tag'] = self.request.arguments.get('tag')[0].decode('utf8')
        
        self.write({'results': ChartSchema().dumps(ChartsClient().get_charts(pk, **params), many=True)})
    
    def post(self, pk: int) -> None:
        """ insert or update charts """
        try:
            body = ChartSchema().loads(self.request.body)  # deserializing Objects
        except ValidationError as err:
            raise HTTPError(400, str(err.messages))
        except json.JSONDecodeError:
            raise HTTPError(500, 'json decode error')
        
        ChartsClient().update_or_insert_chart(pk, body, update=True if pk else False)
        self.write({'message': 'Chart with id {} was update'.format(pk) if pk else 'Chart was insert'})

    def delete(self, pk: int) -> None:
        ChartsClient().delete(pk)
        self.write({'message': 'Chart with id %s was deleted' % pk})


def make_app():
    return Application([
        url("/", InfoProject),
        url(r"/api/chart/([^/]+)?", TodoChart, name='chart')
    ], debug=True)


if __name__ == '__main__':
    app = make_app()
    app.listen(3000)
    IOLoop.instance().start()
