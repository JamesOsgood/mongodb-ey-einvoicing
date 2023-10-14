import os
from datetime import datetime, timedelta
import xmltodict
from pysys.constants import FOREGROUND

from EYBaseTest import EYBaseTest
from datetime import datetime
import random

class PySysTest(EYBaseTest):
	def __init__ (self, descriptor, outsubdir, runner):
		EYBaseTest.__init__(self, descriptor, outsubdir, runner)

		self.importers = {}
		self.importers['R'] = self.import_invoice
		self.BATCH_SIZE = 100
		self.docs = []
		self.doc_count = 0

		self.states = ['Processed_EY', 'Error_EY', 'Processed_MOF', 'Error_MOF', 'InProgress_MOF']
		self.sources = ['Web', 'SFTP', 'Async', 'Sync']
		self.create_queryable_field_defs()

		self.create_conversion_fields()

	# Test entry point
	def execute(self):
		# Gets the connection string from unix.properties and connects to MongoDB
		db = self.get_db_connection()
		self.clear_all(db)
		sub_dir = 'all'
		# sub_dir = 'sample'
		# Data path is defined in unix.properties and defaults to ~/data/mongodb/ey
		data_dir = os.path.join(os.path.expanduser(self.project.DATA_PATH), sub_dir)

		self.process_dir(db, data_dir)
		
	# Iterate through all xml files in dir and process then via import_invoice
	def process_dir(self, db, data_dir):
		self.log.info(f'Processing dir {data_dir}')
		for filename in os.listdir(data_dir):
			full_path = os.path.join(data_dir, filename)
			if os.path.isdir(full_path):
				self.process_dir(db, full_path)
			else:
				if filename.endswith("xml"):
					# self.log.info(f'Processing file {filename}')
					for stub in self.importers.keys():
						if filename.startswith(stub):
							with open(os.path.join(data_dir, filename)) as file:					
								self.importers[stub](db, file)
		self.done_dir(data_dir, db)

	# This is just a plugin to xmltodict to convert xml text values to a mongodb field
	def post_process(self, path, key, value):
		if key.startswith('@'):
			key = key[1:]
		elif key == '#text':
			key = 'value'
		return key, value

	# bulk_inserts any docs left
	def done_dir(self, data_dir, db):
		if len(self.docs) > 0:
			self.doc_count += len(self.docs)
			db.invoice.insert_many(self.docs)
			self.log.info(f"Finished processing {data_dir} - Inserted {self.doc_count}")
			self.docs = []

	# We are using bulk inserts to this just adds a doc to an array and bulk_inserts if necessary
	def add_doc(self, db, doc):
		self.docs.append(doc)
		if len(self.docs) == self.BATCH_SIZE:
			self.doc_count += len(self.docs)
			db.invoice.insert_many(self.docs)
			self.log.info(f"Inserted {self.doc_count}")
			self.docs = []

	# Import an Xml file
	# 	converts it to json
	# 	strips namespace info
	# 	self.post_process to convert a few things
	# 	calls post_process_doc to populate the fake data to mimic the MOF response etc
	def import_invoice(self, db, file):
		namespaces={'urn:oasis:names:specification:ubl:schema:xsd:Invoice-2':None,
			        'urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2' : None,
					"urn:oasis:names:specification:ubl:schema:xsd:QualifiedDataTypes-2" : None,
					"urn:un:unece:uncefact:documentation:2"  : None,
					"urn:oasis:names:specification:ubl:schema:xsd:UnqualifiedDataTypes-2"  : None,
					"urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2"  : None,
					"urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2"  : None }
		doc = xmltodict.parse(file.read(), process_namespaces=True, namespaces = namespaces, postprocessor=self.post_process)
		self.post_process_doc(doc)
		self.add_doc(db, doc)

	def clear_all(self, db):
		db.invoice.drop()

	def post_process_doc(self, doc):
		# Convert types
		for field, convert_func in self.conversion_fields.items():
			parent, field_name = self.get_immediate_parent_and_field_name(doc, field.split('|'))
			convert_func(parent, field_name)

		# Create the mock data
		doc['header'] = self.create_header(doc)
		doc['mof_response'] = self.create_mof_response(doc)
		
		# Collect all queryable fields into 1 section
		doc['queryable'] = self.create_queryable_fields(doc)
		self.create_invoice_key(doc)

	# Invoice Key is combination of Invoice Type , Document Number, Document Date, Seller VAT ID or Seller VAT Registration
	def create_invoice_key(self, doc):
		invoice = doc['Invoice']
		queryable = doc['queryable']
		mof = doc['mof_response']

		parts = []
		parts.append(invoice['InvoiceTypeCode'])
		parts.append(invoice['ID'])
		parts.append(invoice['IssueDate_str'])
		
		issif = 2
		if 'IsSif' in mof:
			issif = int(mof['IsSif'])
		
		if issif == 2:
			parts.append(queryable['svatid'])
		else:
			parts.append(queryable['cvatid'])

		queryable['invoice_key'] = '|'.join(parts)

	# Fake the information from the first stage of processing of an invoice
	def create_header(self, doc):
		header = {}
		header['status'] = random.choice(self.states)
		header['source'] = random.choice(self.sources)
		header['invoice_upload_date'] = datetime.today()
		return header

	# Fake the information from the stage of processing where the invoice is sent to MOF
	def create_mof_response(self, doc):
		response = {}
		response['eDate'] = doc['header']['invoice_upload_date'] + timedelta(hours=1)
		response['DocDate'] = doc['Invoice']['IssueDate']
		response['Action'] = 'Status Details'
		
		if random.random() > 0.33:
			response['IsSif'] = random.choice([1,2])

		return response
	
	# Gather all the fields we want to query into a convenient section
	def create_queryable_fields(self, doc):
		query = {}
		for name, path in self.queryable_field_defs.items():
			if path is None:
				query[name] = parent[field_name] = '<Unknown query field>'
			else:
				parent, field_name = self.get_immediate_parent_and_field_name(doc, path.split('|'))
				query[name] = parent[field_name]
		
		return query
	
	# Helper methods
	def create_conversion_fields(self):
		self.conversion_fields = {}
		self.conversion_fields['Invoice|IssueDate'] = self.convert_date

	def create_queryable_field_defs(self):
		self.queryable_field_defs = {}
		self.queryable_field_defs['invoice_upload_date'] = 'header|invoice_upload_date'
		self.queryable_field_defs['document_date'] = 'Invoice|IssueDate'
		self.queryable_field_defs['mof_date'] = 'mof_response|eDate'
		self.queryable_field_defs['cvatid'] = 'Invoice|AccountingCustomerParty|Party|PartyTaxScheme|CompanyID'
		self.queryable_field_defs['svatid'] = 'Invoice|AccountingSupplierParty|Party|PartyTaxScheme|CompanyID'
		self.queryable_field_defs['invoice_type'] = 'Invoice|InvoiceTypeCode'
		self.queryable_field_defs['status'] = 'header|status'
		self.queryable_field_defs['source'] = 'header|source'
	
	# Convert string to json date
	def convert_date(self, parent, field_name):
		value = parent[field_name]
		parent[field_name + '_str'] = value
		parent[field_name] = datetime.strptime(value, '%Y-%m-%d')

	def validate(self):
		pass

	
	
