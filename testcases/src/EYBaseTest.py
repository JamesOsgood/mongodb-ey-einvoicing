
import os
import time

from pymongo import MongoClient
from pysys.basetest import BaseTest
from pysys.constants import FOREGROUND

class EYBaseTest(BaseTest):
	def __init__ (self, descriptor, outsubdir, runner):
		BaseTest.__init__(self, descriptor, outsubdir, runner)

		self.db_connection = None
		self.connectionString = self.project.CONNECTION_STRING.replace("~", "=")


	# open db connection
	def get_db_connection(self, dbname = None):
		if self.db_connection is None:
			self.log.info("Connecting to: %s" % self.connectionString)
			client = MongoClient(self.connectionString)
			if dbname:
				self.db_connection = client.get_database(dbname)
			else:
				self.db_connection = client.get_database()

		return self.db_connection

	# Run query
	def run_query(self, filter):
		db = self.get_db_connection()
		self.log.info(filter)
		for ret in db.docs.find(filter):
			self.log.info(f"Query returned documentid {ret['_id']}")

	def run_agg_query(self, pipeline):
		db = self.get_db_connection()
		for ret in db.docs.aggregate(pipeline):
			self.log.info(ret['_id'])

	def importFileMongoImport(self, filePath, collection, type='csv', dropCollection=True, connectionString = None, ignore_blanks=False, jsonArray=False, columnsHaveTypes=False):

		if not connectionString:
			connectionString = self.connectionString

		args = []
		if dropCollection:	
			args.append('--drop')

		if ignore_blanks:
			args.append('--ignoreBlanks')

		if columnsHaveTypes:
			args.append('--columnsHaveTypes')
		args.append(f'--type={type}')
		if type == 'csv' or type == 'tsv':
			args.append('--headerline')
		args.append('--numInsertionWorkers=8')
		args.append(f'--collection={collection}')
		args.append(f'--file={filePath}')
		args.append(f'--uri="{connectionString}"')
		if jsonArray:
			args.append('--jsonArray')

		command = self.project.MONGOIMPORT
		self.log.info("%s %s" % (command, " ".join(args)))
		
		self.startProcess(command, args, state=FOREGROUND, stdout='mongoimport_out.log', stderr='mongoimport_err.log', timeout=360000 )

	def get_immediate_parent_and_field_name(self, doc, fields):
		parent = doc
		index = 0
		for index in range(len(fields) - 1):
			field_name = fields[index]
			if field_name in parent:
				parent = parent[field_name]
			else:
				return None, None

		index += 1

		return (parent, fields[index])
	
	def get_field_value(self, doc, delimited_value_path):
		parent, field_name = self.get_immediate_parent_and_field_name(doc, delimited_value_path.split('|'))
		if parent != None:
			if field_name in parent:
				return parent[field_name]
			else:
				return None
		
		else:
			return None



