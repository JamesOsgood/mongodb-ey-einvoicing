import os
from datetime import datetime, timedelta
from pysys.constants import FOREGROUND

from EYBaseTest import EYBaseTest
from datetime import datetime
import csv

class PySysTest(EYBaseTest):
	def __init__ (self, descriptor, outsubdir, runner):
		EYBaseTest.__init__(self, descriptor, outsubdir, runner)

	# Test entry point
	def execute(self):
		# Get the mappings from the file
		self.open_mappings_file()

		# Gets the connection string from unix.properties and connects to MongoDB
		db = self.get_db_connection()

		missing_paths = {}
		for doc in db.invoice.find():
			self.check_fields_exist_in_doc(doc, missing_paths)

		self.log.info(f'Missing {len(missing_paths)} from {len(self.fields)}')
		for path in missing_paths:
			self.log.info(f'{path}({missing_paths[path]})')

		
	def check_fields_exist_in_doc(self, doc, missing_paths):
		for field_name in self.fields:
			field = self.fields[field_name]
			if field:
				if not field['is_computed']:
					field_path = field['path']
					value = self.get_field_value(doc, field_path)
					if value is None:
						if field_path not in missing_paths:
							missing_paths[field_path] = 1
						else:
							missing_paths[field_path] = missing_paths[field_path] + 1


	
	def open_mappings_file(self):
		path = os.path.join(self.input, 'romania_staging_mapping.csv')
		header = None
		self.fields = {}
		row_num = 1
		with open(path) as csvfile:
			mapping_file = csv.reader(csvfile)
			for row in mapping_file:
				if header is None:
					# Data Field name in the Staging File, Cardinalit, xml tag
					header = row
				else:
					field_name = row[0]
					if field_name == '-':
						field_name = f'CONSTANT_{row_num}'
					field_path, comment, is_computed = self.process_xml_tag(row[2])
					field_desc = None
					if field_path:
						field_desc = {}
						field_desc['path'] = field_path
						if comment:
							field_desc['comment'] = comment
						field_desc['is_computed'] = is_computed
					self.fields[field_name] = field_desc
				row_num += 1

		# for f in self.fields:
		# 	self.log.info(f'{f} - {self.fields[f]}')


	def process_xml_tag(self, input_tag):
		output_path = None
		comment = None
		is_computed = False
		if input_tag != '-' and len(input_tag) > 0:
			output_parts = ['Invoice']
			parts = input_tag.split('/')
			for part in parts:
				part = part.strip()
				colon_index = part.find(':')
				
				if colon_index != -1:
					part = part[colon_index+1:]

				output_parts.append(part)

			# Process value
			value_index = len(output_parts) -1
			value = output_parts[value_index]

			if value.startswith('@'):
				value = value[1:]

			# Comments
			sub_parts = value.split('\n')
			if len(sub_parts) > 1:
				value = sub_parts[0]
				comment = sub_parts[1]

			# Condition
			if value.find('=') != -1:
				is_computed = True

			output_parts[value_index] = value
			output_path = '|'.join(output_parts)

		return output_path, comment, is_computed


	def validate(self):
		pass

	
	
