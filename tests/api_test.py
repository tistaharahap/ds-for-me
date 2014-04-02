import api
import unittest


class ApiTestCase(unittest.TestCase):

	def setUp(self):
		self.app = api.app.test_client()

	def test_welcome_ok(self):
		resp = self.app.get('/')

		self.assertIs(int(resp.status_code), 200, msg='Successful retrieval must return HTTP 200')