#!/usr/bin/python
# -*- coding: utf-8 -*-
import MySQLdb as mdb
import pyodbc
import pprint
import sys, re
import xml.etree.ElementTree as ET
import io
import base64
import os
import logging
import cgi
import pudb
from transfer_sql import configuration, dc_data, insert_data
from collections import defaultdict
from sections import sections

VERBOSE = 1

path_to_barcodes = "barcodes"  # -- "/var/www/biocase/biocase_media/barcodes/gbol"

def inputnumber():
   num = raw_input('Enter ProjectID(s): ').split(',')
   if num[0]=='':
	   num=['640']
   return [str(n) for n in num]

def sql_clean(s):
	if not s:
		return ""
	return s.replace("\'","'").replace("'","\\\'").replace('"','\\\"').replace('&','and')

class SectionName():
	def __init__(self, project_ids=()):
		self.name = None
		self.project_ids = project_ids
		for sect in sections:
			for e in sections[sect]['collections']:
				if str(e['dwb_id']) in project_ids:
					self.name = sect
		if self.name==None:
			raise NameError, 'No section for this project ID (%r)' % project_ids
	
	def get_project_ids(self):
		return self.project_ids
		
	def get_section_name(self):
		return self.name
		
	def get_section_id(self):
		return sections[self.name]['dwb_id']

class DWBBase():
	def __init__(self, db_name):
		self.con = self.__odbc_connect(db_name)

	def __odbc_connect(self, db):
		dbs = {"coll_zfmk": "DSN=<DATASource>;UID=<UserID>;PWD=<Password>"}
		cnxn = pyodbc.connect(dbs[db])
		return cnxn

class DC_Project(DWBBase):
	def __init__(self, section):
		DWBBase.__init__(self, 'coll_zfmk')
		self.query = dc_data(section=section)
		self.data = {}
		self.length = 0
		self.getData()

	def getData(self):
		resU = {}
		odbc_csr = self.con.cursor()
		odbc_csr.execute(self.query.project_proxy())
		self.data = odbc_csr.fetchall()
		self.length = len(self.data)
		return self.data

	def yield_items(self):
		for p in self.data:
			yield {'ProjectID': p[0], 'Project': sql_clean(p[1])}

class DC_BaseURL(DWBBase):
	def __init__(self, section):
		DWBBase.__init__(self, 'coll_zfmk')
		self.query = dc_data(section=section)
		self.data = {}
		self.length = 0
		self.getData()

	def getData(self):
		resU = {}
		odbc_csr = self.con.cursor()
		odbc_csr.execute(self.query.base_url())
		self.data = odbc_csr.fetchall()
		self.length = len(self.data)
		return self.data

	def yield_items(self):
		for p in self.data:
			yield {'text': "%s" % p[0]}

class DC_Data(DWBBase):
	def __init__(self, section):
		DWBBase.__init__(self, 'coll_zfmk')
		self.query = dc_data(section=section)
		self.data = {}
		self.clean = self.__fkt_clean
		self.is_empty = self.__fkt_is_empty
		self.current_table=''

	def __fkt_clean(self, value):
		try:
			s = value.strip()
			return u'"'+sql_clean(s)+u'"'
		except AttributeError, e:
			if value is None:
				return 'NULL'
			return value
		else:
			return value

	def __fkt_is_empty(self, value):
		if isinstance(value, basestring) and (len(value)==0 or value=='NULL'):
			return True
		if value is None:
			return True

	def getAvailableDBTables(self,database_table="", page=-1):
		self.current_table = database_table
		if database_table=='Project':
			return self.query.project(page)
		if database_table=='CollectionProject':
			return self.query.collection_project(page)
		elif database_table=='CollectionSpecimen':
			return self.query.specimen(page)
		elif database_table=='CollectionSpecimenPart':
			return self.query.specimen_parts(page)
		elif database_table=='IdentificationUnit':  # -- Organism
			return self.query.identification_unit(page)
		elif database_table=='IdentificationUnitGeoAnalysis':
			return self.query.identification_unit_geoanalysis(page)
		elif database_table=='IdentificationUnitInPart':
			return self.query.identification_unit_part(page)
		elif database_table=='Identification':
			return self.query.identification(page)
		elif database_table=='CollectionAgent':
			return self.query.collection_agents(page)
		elif database_table=='CollectionEvent':
			return self.query.event(page)
		elif database_table=='CollectionEventLocalisation':
			return self.query.event_localisation(page)
		elif database_table=='LocalisationSystem':
			return self.query.localisation_system(page)
		elif database_table=='CollectionSpecimenImage':
			return self.query.specimen_image(page)
		elif database_table=='CollectionSpecimenRelation':
			return self.query.specimen_relation(page)
		elif database_table=='IdentificationUnitAnalysis':
			return self.query.identification_unit_analysis(page)
		elif database_table=='Barcoding':
			return self.query.identification_unit_analysis_barcoding(page)
		else:
			return ['CollectionSpecimen', 'CollectionSpecimenPart', 'IdentificationUnit', 'IdentificationUnitGeoAnalysis', 'IdentificationUnitInPart', 'Identification', 'CollectionAgent', 'CollectionEvent', 'CollectionEventLocalisation', 'LocalisationSystem', 'CollectionSpecimenImage', 'CollectionSpecimenRelation', 'IdentificationUnitAnalysis', 'Barcoding', 'CollectionProject']

	def getData(self, database_table, page):
		resU = []
		U = resU.append
		clean = self.clean
		odbc_csr = self.con.cursor()
		i = 0
		try:
			odbc_csr.execute(self.getAvailableDBTables(database_table, page))
		except Exception, e:
			s = e
			s+= self.getAvailableDBTables(database_table)
			logging.error(s)

		for row in odbc_csr.fetchall():
			i = row[0]
			if (database_table=='CollectionEvent' and self.is_empty(row[1])) \
				or (database_table=='CollectionSpecimen' and self.is_empty(row[3])) \
				or (database_table=='CollectionEventLocalisation' and self.is_empty(row[1])):
				continue
			r = {t[0]: clean(value) for (t, value) in zip(odbc_csr.description, row)}
			s = "%07i got (%s...)" % (i, repr(r)[:120].replace('"',''))
			logging.info(s)
			U(r)
		self.data = resU
		logging.info("Length of (%s) %i" % (database_table, len(resU)))

	def length(self):
		return len(self.data)

	def reset(self):
		self.data = {}

	def check_withhold(self, row):
		if 'DataWithholdSpecimen' in row:
			value = row['DataWithholdSpecimen']
			if len(value)>0 and value!='NULL' and value!='""':
				return True
		if 'DataWithholdingReason' in row:
			value = row['DataWithholdingReason']
			if len(value)>0 and value!='NULL' and value!='""':
				return True
		return False

	def yield_items(self):
		is_empty = self.is_empty
		for row in self.data:
			if self.check_withhold(row):
				continue
			if self.current_table == 'Barcoding':
				for x in self.get_xml(row):
					yield x
			else:
				yield row

	def ns_tag(self, tag):
		return str(ET.QName('http://diversityworkbench.net/Schema/tools', tag))

	def get_xml(self, row):
		c = self.__fkt_clean
		x = str(row['ToolUsage'][1:-1]).replace('\\','')
		root = ET.fromstring(x)
		tool = root.findall(self.ns_tag('Tool'))
		for t in tool:
			table_name = t.get('Name')
			tr_dict = {}
			if table_name == 'Barcoding':
				bc_dict = {'BarcodingID':'NULL'}
				bc_dict['CollectionSpecimenID']= row['CollectionSpecimenID']
				bc_dict['IdentificationUnitID']= row['IdentificationUnitID']
				bc_dict['AnalysisID']= row['AnalysisID']
				for usage in t.iter(self.ns_tag('Usage')):
					bc_dict.update([(usage.get('Name'),c(usage.get('Value')))])
			else:
				for key in bc_dict:
					tr_dict.update([(key,bc_dict.get(key))])
				for usage in t.iter(self.ns_tag('Usage')):
					tr_dict.update([(usage.get('Name'),c(usage.get('Value')))])
					if usage.get('Name') == 'trace_filename':
						file_name = (c(usage.get('Value')))
					if usage.get('Name') == 'trace_file_encoded':
						tfe = (c(usage.get('Value')))
						tfe_decoded = base64.b64decode(tfe.replace('"',''))
						with io.open(os.path.join(path_to_barcodes, str(file_name.replace('"',''))), 'wb') as f:
							f.write(tfe_decoded)
							f.close()
				yield tr_dict


form = cgi.FieldStorage()
id_input = []
for i in form.keys():
	id_input.append(form[i].value)
input_project_ids = list(map(str, id_input))

print("Content-type: text/html\n")
print("""
<html>
<head>
	<title>Daten zu BioCASe hochladen</title>
</head>
<body>
<h1 id="header">Datens&auml;tze hochladen</h1>""")

with open('transfer_website.log', 'w'):
	pass
logging.basicConfig(filename='transfer_website.log', level=logging.INFO)
section = SectionName(project_ids = input_project_ids)
insert = insert_data(section)

if VERBOSE>0: logging.info("DWB: get Projects")
try:
	dwb_projects = DC_Project(section)
except Exception, e:
	logging.error(e)
	sys.exit
if VERBOSE>0: logging.info("Insert Projects")
insert.projects(dwb_projects)

if VERBOSE>0: logging.info("DWB: get Base URL")
try:
	dwb_base_url = DC_BaseURL(section)
except Exception, e:
	logging.error(e)
	sys.exit
if VERBOSE>0: logging.info("Insert Base URL")
insert.base_url(dwb_base_url)

if VERBOSE>0: logging.info("DWB: Data")
try:
	dc_data = DC_Data(section)
except Exception, e:
	logging.error(e)
	sys.exit
if VERBOSE>0: logging.info("Insert")
insert.data(dc_data)

print("""
<p><a href="transfer_website.log">Download Log-File</a>
</body>
</html>""")
