from datetime import datetime, timedelta
from pysys.constants import FOREGROUND

from EYBaseTest import EYBaseTest
from datetime import datetime
import json

class PySysTest(EYBaseTest):
	def __init__ (self, descriptor, outsubdir, runner):
		EYBaseTest.__init__(self, descriptor, outsubdir, runner)

	# Test entry point
	def execute(self):
		# Gets the connection string from unix.properties and connects to MongoDB
		db = self.get_db_connection()

		filter = {}
		for key in self.get_invoice_keys(db, 10):
			filter['queryable.invoice_key'] = key
			doc = db.invoice.find_one(filter)
			self.log.info(doc['Invoice']['ID'])

	def get_invoice_keys(self, db, count):
		pipeline = [{
				'$sample': {
					'size': 100
				}
			}, {
				'$project': {
					'_id': 0, 
					'invoice_key' : '$queryable.invoice_key'
				}
			}
		]

		docs = db.invoice.aggregate(pipeline)
		ids = []
		for doc in docs:
			ids.append(doc['invoice_key'])

		return ids
	
	def validate(self):
		pass

	
	
