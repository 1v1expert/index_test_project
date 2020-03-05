from tornado.testing import AsyncHTTPTestCase, AsyncTestCase, AsyncHTTPClient
from pygintest import make_app, ChartSchema


class TestWorkApp(AsyncHTTPTestCase):
    def get_app(self):
        return make_app()
    
    def test_homepage(self):
        response = self.fetch('/')
        self.assertEqual(response.code, 200)
        self.assertIn('link to charts', str(response.body))

    def test(self):
        self.assertTrue(hasattr(self, "_app"))
        self.assertTrue(hasattr(self, "http_server"))


class TestChart(AsyncHTTPTestCase):
    def setUp(self):
        super(TestChart, self).setUp()
        import random
        self.id = random.randint(99999, 9999999)
    
    def get_app(self):
        return make_app()
    
    def test_get_chart(self):
        pass
        
    def test_insert(self):
        import json
        post_body = {"id": self.id, "tags": ["tag1", "tag2"]}
        
        # test insert chart
        insert_chart = self.fetch("/api/chart/", method="POST", body=json.dumps(post_body))
        self.assertEqual(insert_chart.code, 200)
        self.assertIn('OK', str(insert_chart.body))
        
        # test get chart
        get_chart = self.fetch("/api/chart/{}".format(self.id))
        self.assertEqual(get_chart.code, 200)
        self.assertDictEqual({'results': [post_body]}, json.loads(get_chart.body))#ChartSchema().load(str(response.body)))
        
        # test update chart
        update_body = post_body
        update_body["tags"] = ["tag3", "tag4", "tag5"]
        update_chart = self.fetch("/api/chart/{}".format(self.id), method="POST", body=json.dumps(update_body))
        self.assertEqual(update_chart.code, 200)
        
        # test get update body chart
        get_update_chart = self.fetch("/api/chart/{}".format(self.id))
        self.assertEqual(get_update_chart.code, 200)
        self.assertDictEqual({'results': [update_body]}, json.loads(get_update_chart.body))
        self.assertIn('OK', str(insert_chart.body))
        
        # test delete chart
        delete_chart = self.fetch("/api/chart/{}".format(self.id), method="DELETE")
        self.assertEqual(delete_chart.code, 200)
        self.assertEqual({'message': 'Chart with id %s was deleted' % self.id}, json.loads(delete_chart.body))
        
    
    

