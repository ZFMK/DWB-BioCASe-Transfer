#!/usr/bin/python
# -*- coding: utf-8 -*-

import MySQLdb as mdb
import pyodbc
import pprint
import sys, re
import warnings
import pudb
import logging
from sections import sections

use_db = 'local'


configuration = {'db_pre': 'ZFMK_BioCASe_',
	'dbs': {'local': {'host': "localhost",
		'user': "<DB-Username>",
		'passwd': "<DB_Password",
		'database': "ZFMK_BioCASe_Template"},
			'fredie': {'host': "localhost",
		'user': "DB-Udername",
		'passwd': "DB-Password",
		'database': "ZFMK_BioCASe_Template"}
	}
}

class ProjectDB(object):
	def __init__(self, section):
		self.section = section
		self.con = self.__mysql_connect()
		self.cur = self.con.cursor()
		self.con.autocommit(True)
		self.execute = self.__execute
		self.commit = self.__commit
		self.__get_db_name()
		self.create_scheme()

	def __mysql_connect(self, db_name=None):
		dbs = configuration['dbs'][use_db]
		if not db_name:
			db_name = dbs['database']
		try:
			con = mdb.connect(host=dbs['host'], user=dbs['user'], passwd=dbs['passwd'], db=db_name)
		except mdb.Error, e:
			logging.error("Error %d: %s" % (e.args[0],e.args[1]))
			sys.exit(1)
		return con

	def __execute(self, query):
		with warnings.catch_warnings():
			warnings.simplefilter('error', mdb.Warning)
			try:
				self.cur.execute(query)
				self.__commit()
			except mdb.Warning, e:
				logging.warning("Query: %s" % query)
				logging.warning("MySQL: %r" % e.args)
			except mdb.Error, e:
				pudb.set_trace()
				logging.error("Query: %s" % query)
				logging.error("MySQL Error [{0}]: {1}".format(e.args[0], e.args[1]))
				sys.exit(1)
			except Exception, e:
				logging.error("Query: %s" % query)
				logging.error("MySQL Error %r" % e)
				sys.exit(1)

	def __commit(self):
		try:
			self.con.commit()
		except mdb.Error, e:
			if self.con:
				self.con.rollback()
			logging.error("Error %d: %s" % (e.args[0],e.args[1]))
			sys.exit(1)

	def final(self):
		if self.con:
			self.con.close()

	def __get_db_name(self):
		section_id = self.section.get_section_id()
		q = """SELECT replace(replace(databasename, " ",""), "-","_") as databasename from `ZFMK_BioCASE_Data`.`T_BIOCASE_database` d
				right join `ZFMK_BioCASE_Data`.`T_BIOCASE_collection` c on c.databaseid=d.databaseid
			where c.dc_project = %s
			group by c.databaseid;""" % self.section.get_section_id()
		self.cur.execute(q)
		for row in self.cur.fetchall():
			self.db_name = row[0]
		self.new_db = "%s%s" % (configuration['db_pre'],self.db_name)

	def create_scheme(self, reset=False):
		tables = []
		views = {}
		source_db = configuration['dbs'][use_db]['database']
		db_user = configuration['dbs'][use_db]['user']
		q1 = """DROP DATABASE IF EXISTS `%s`""" % self.new_db
		q2 = """Create Database if not exists %s""" % self.new_db
		q3 = """SHOW FULL TABLES where Table_type like '%TABLE%'"""
		q4 = """SHOW FULL TABLES where Table_type like '%VIEW%'"""
		q5 = """CREATE TABLE IF NOT EXISTS {0}.`{1}` like {1}"""
		q6 = """DROP VIEW IF EXISTS `{0}`"""
		q7 = """SHOW CREATE VIEW `{0}`"""
		q8 = """USE `{0}`""".format(self.new_db)

		if reset:
			print q1
			self.execute(q1)
		
		logging.info(q2)
		self.execute(q2)
		
		self.execute(q3)
		for row in self.cur.fetchall():
			tables.append(row[0])
	
		self.execute(q4)
		for row in self.cur.fetchall():
			views[row[0]] = ""
		
		for table in tables:
			q = q5.format(self.new_db, table)
			self.execute(q)
		
		for view in views.keys():
			q = q7.format(view)
			self.execute(q)
			for row in self.cur.fetchall():
				views[view] = row[1].replace(source_db,self.new_db).replace('VIEW ','VIEW %s.' % self.new_db).replace("DEFINER=`root`@`10.10.8.%`", "DEFINER=CURRENT_USER")
		
		self.execute(q8)
		
		for view in views.keys():
			q = q6.format(view)
			self.execute(q)
			q = views[view]
			self.execute(q)
		
"""
	All Queries for data from DiversityCollection
"""
class dc_data():
	""" all queries related to CollectionSpecimen DB
			was collection_specimen """
	def __init__(self, section):
		projects_ids = section.get_project_ids()
		if section.get_section_name()=='gbol':
			self.projects = "p.ProjectID>=%i and p.ProjectID<=%i" % (projects_ids[0], projects_ids[1])
		elif len(projects_ids)==1:
			self.projects = "p.ProjectID = %s" % projects_ids[0]
		else:
			self.projects = "p.ProjectID in (%s)" % ", ".join([str(i) for i in projects_ids])

	def base_url(self):
		""" in <Project>_Data """
		return """SELECT cast(dbo.BaseURL() as varchar(255)) as BaseURL"""

	def project_proxy(self):
		""" in <Project>_Data """
		return """SELECT p.ProjectID,
			cast(p.Project as varchar(50)) as Project
		from ProjectProxy p
		where %s""" % self.projects

	def collection_project(self, page=0):
		start = (page*10)+1
		end = (page+1)*10
		""" <Project>-data with specimen """
		return """SELECT * FROM (
			SELECT ROW_NUMBER() OVER (ORDER BY p.CollectionSpecimenID) as row_no,
			p.CollectionSpecimenID,
			p.ProjectID,
			cast(c.DataWithholdingReason as varchar(50)) as DataWithholdSpecimen
		from CollectionProject p
			inner join CollectionSpecimen c on c.CollectionSpecimenID=p.CollectionSpecimenID
		where %s
		) p where p.row_no>=%i and p.row_no<=%i""" % (self.projects, start, end)

	def project(self, page=0):
		start = (page*10)+1
		end = (page+1)*10
		""" <Project>-data with specimen """
		return """SELECT * FROM (
			SELECT  ROW_NUMBER() OVER (ORDER BY p.ProjectID) as row_no,
					p.ProjectID,
					cast(p.Project as varchar(50)) as [Project],
					cast(p.ProjectTitle as varchar(200)) as [ProjectTitle],
					cast(p.ProjectEditors as varchar(255)) as [ProjectEditors],
					cast(p.ProjectInstitution as varchar(500)) as [ProjectInstitution],
					cast(p.ProjectNotes as varchar(max)) as [ProjectNotes],
					cast(p.ProjectVersion as varchar(255)) as [ProjectVersion],
					cast(p.ProjectCopyright as varchar(255)) as [ProjectCopyright],
					cast(p.ProjectURL as varchar(255)) as [ProjectURL],
					cast(p.ProjectSettings as varchar(max)) as [ProjectSettings],
					cast(p.ProjectRights as varchar(255)) as [ProjectRights],
					cast(p.ProjectLicenseURI as varchar(255)) as [ProjectLicenseURI],
					cast(p.RowGUID as varchar(50)) as [RowGUID]
			  FROM DiversityProjects_ZFMK.dbo.Project p
		where %s
		) p where p.row_no>=%i and p.row_no<=%i""" % (self.projects, start, end)

	def specimen(self, page=0):
		start = (page*10)+1
		end = (page+1)*10
		""" Specimen with <Project>_Data """
		return """SELECT TOP 100 PERCENT * FROM (
			SELECT ROW_NUMBER() OVER (ORDER BY c.CollectionSpecimenID) as row_no,
			c.CollectionSpecimenID,
			c.CollectionEventID,
			cast(c.AccessionNumber as varchar(50)) as AccessionNumber,
			left(convert(varchar, c.AccessionDate, 120),25) as AccessionDate,
			c.AccessionDay,
			c.AccessionMonth,
			c.AccessionYear,
			cast(c.AccessionDateSupplement as varchar(255)) as AccessionDateSupplement,
			cast(c.DepositorsName as varchar(255)) as DepositorsName,
			cast(c.DepositorsAgentURI as varchar(255)) as DepositorsAgentURI,
			cast(c.DepositorsAccessionNumber as varchar(255)) as DepositorsAccessionNumber,
			cast(c.OriginalNotes as varchar(max)) as OriginalNotes,
			cast(c.AdditionalNotes as varchar(max)) as AdditionalNotes,
			cast(c.ReferenceTitle as varchar(255)) as ReferenceTitle,
			cast(c.ReferenceURI as varchar(255)) as ReferenceURI,
			cast(c.ReferenceDetails as varchar(50)) as ReferenceDetails,
			cast(c.DataWithholdingReason as varchar(50)) as DataWithholdingReason,
			cast(c.RowGUID as varchar(50)) as RowGUID
		from CollectionSpecimen c inner join (
			select s.CollectionSpecimenID from CollectionSpecimen s
				inner join IdentificationUnit iu on s.CollectionSpecimenID=iu.CollectionSpecimenID
				inner join IdentificationSequenceMax i on i.IdentificationUnitID=iu.IdentificationUnitID
				inner join CollectionProject p on p.CollectionSpecimenID=s.CollectionSpecimenID
			where %s
			group by s.CollectionSpecimenID
		) as p on p.CollectionSpecimenID=c.CollectionSpecimenID
		) p where p.row_no>=%i and p.row_no<=%i""" % (self.projects, start, end)

	def specimen_parts(self, page=0):
		start = (page*10)+1
		end = (page+1)*10
		""" Specimen with <Project>_Data """
		return """SELECT TOP 100 PERCENT * FROM (
			SELECT ROW_NUMBER() OVER (ORDER BY sp.CollectionSpecimenID) as row_no,
			sp.CollectionSpecimenID,
			sp.SpecimenPartID,
			sp.DerivedFromSpecimenPartID,
			cast(CASE WHEN sp.AccessionNumber IS NULL OR
				sp.AccessionNumber = '' THEN s.AccessionNumber ELSE sp.AccessionNumber END as varchar(50)) as AccessionNumber,
			cast(sp.PreparationMethod as varchar(max)) as PreparationMethod,
			left(convert(varchar, sp.PreparationDate, 120),25) as PreparationDate,
			cast(sp.PartSublabel as varchar(50)) as PartSublabel,
			sp.CollectionID,
			cast(sp.MaterialCategory as varchar(255)) as MaterialCategory,
			cast(sp.StorageLocation as varchar(255)) as StorageLocation,
			sp.Stock,
			cast(sp.Notes as varchar(max)) as Notes,
			cast(sp.StorageContainer as varchar(500)) as StorageContainer,
			cast(sp.StockUnit as varchar(50)) as StockUnit,
			cast(sp.ResponsibleName as varchar(255)) as ResponsibleName,
			cast(sp.ResponsibleAgentURI as varchar(255)) as ResponsibleAgentURI,
			cast(sp.DataWithholdingReason as varchar(255)) as DataWithholdingReason,
			cast(s.DataWithholdingReason as varchar(50)) as DataWithholdSpecimen,
			cast(sp.RowGUID as varchar(50)) as RowGUID
		FROM IdentificationUnitInPart iup
			inner join IdentificationSequenceMax i on i.IdentificationUnitID=iup.IdentificationUnitID
			inner join CollectionSpecimenPart sp on sp.SpecimenPartID=iup.SpecimenPartID
			left join CollectionSpecimen s on sp.CollectionSpecimenID = s.CollectionSpecimenID
			inner join CollectionProject p on p.CollectionSpecimenID=s.CollectionSpecimenID
		WHERE (sp.AccessionNumber <> N'' or s.AccessionNumber <> N'') and %s
		) p where p.row_no>=%i and p.row_no<=%i""" % (self.projects, start, end)

	def identification_unit(self, page=0):
		start = (page*10)+1
		end = (page+1)*10
		""" the Organisms, the "root" of all """
		return """SELECT * FROM (
			SELECT ROW_NUMBER() OVER (ORDER BY iu.CollectionSpecimenID) as row_no,
				iu.CollectionSpecimenID,
				iu.IdentificationUnitID,
				cast(iu.LastIdentificationCache as varchar(255)) as LastIdentificationCache,
				cast(iu.FamilyCache as varchar(255)) as FamilyCache,
				cast(iu.OrderCache as varchar(255)) as OrderCache,
				cast(iu.TaxonomicGroup as varchar(50)) as TaxonomicGroup,
				iu.OnlyObserved,
				cast(iu.LifeStage as varchar(255)) as LifeStage,
				cast(iu.Gender as varchar(50)) as Gender,
				iu.NumberOfUnits,
				cast(iu.UnitIdentifier as varchar(50)) as UnitIdentifier,
				cast(iu.UnitDescription as varchar(50)) as UnitDescription,
				cast(iu.Circumstances as varchar(255)) as Circumstances,
				iu.DisplayOrder,
				cast(iu.Notes as varchar(max)) as Notes,
				cast(iu.RowGUID as varchar(50)) as RowGUID,
				cast(iu.HierarchyCache as varchar(500)) as HierarchyCache,
				iu.ParentUnitID,
				cast(s.DataWithholdingReason as varchar(50)) as DataWithholdSpecimen
			from IdentificationUnit iu
				inner join IdentificationSequenceMax i on i.IdentificationUnitID=iu.IdentificationUnitID
				inner join CollectionProject p on p.CollectionSpecimenID=iu.CollectionSpecimenID
				left join CollectionSpecimen s on iu.CollectionSpecimenID = s.CollectionSpecimenID
			where %s
			) p where p.row_no>=%i and p.row_no<=%i""" % (self.projects, start, end)

	def identification_unit_geoanalysis(self, page=0):
		start = (page*10)+1
		end = (page+1)*10
		""" Georeference of identification units """
		return """SELECT * FROM (
			SELECT ROW_NUMBER() OVER (ORDER BY iug.CollectionSpecimenID) as row_no,
				iug.CollectionSpecimenID,
				iug.IdentificationUnitID,
				left(convert(varchar, iug.AnalysisDate, 120),25) as AnalysisDate,
				cast(iug.Geography.Lat as float) as GeoLat,
				cast(iug.Geography.Long as float) as GeoLong,
				cast(iug.ResponsibleName as varchar(255)) as ResponsibleName,
				cast(iug.ResponsibleAgentURI as varchar(255)) as ResponsibleAgentURI,
				cast(iug.Notes as varchar(max)) as Notes
				, cast(s.DataWithholdingReason as varchar(50)) as DataWithholdSpecimen
				, cast(iug.RowGUID as varchar(50)) as RowGUID
			from IdentificationUnitGeoAnalysis iug
				inner join IdentificationUnit iu on iu.IdentificationUnitID=iug.IdentificationUnitID
				inner join IdentificationSequenceMax i on i.IdentificationUnitID=iu.IdentificationUnitID
				inner join CollectionProject p on p.CollectionSpecimenID=iu.CollectionSpecimenID
				left join CollectionSpecimen s on iu.CollectionSpecimenID = s.CollectionSpecimenID
			where %s
			) p where p.row_no>=%i and p.row_no<=%i""" % (self.projects, start, end)

	def identification_unit_part(self, page=0):
		start = (page*10)+1
		end = (page+1)*10
		""" link from SpecimenPart to IdentificationUnit """
		return """SELECT * FROM (
			SELECT ROW_NUMBER() OVER (ORDER BY i.CollectionSpecimenID) as row_no,
				iup.CollectionSpecimenID,
				iup.IdentificationUnitID,
				iup.SpecimenPartID,
				iup.DisplayOrder,
				cast(iup.Description as varchar(max)) as Description,
				cast(s.DataWithholdingReason as varchar(50)) as DataWithholdSpecimen,
				cast(iup.RowGUID as varchar(50)) as RowGUID
			FROM IdentificationUnitInPart iup
				inner join IdentificationSequenceMax i on iup.IdentificationUnitID=i.IdentificationUnitID
				inner join CollectionProject p on p.CollectionSpecimenID=iup.CollectionSpecimenID
				left join CollectionSpecimen s on iup.CollectionSpecimenID = s.CollectionSpecimenID
			where %s
			) p where p.row_no>=%i and p.row_no<=%i""" % (self.projects, start, end)

	def identification(self, page=0):
		start = (page*10)+1
		end = (page+1)*10
		""" specimen identifications für <Project>_Taxa """
		return """SELECT * FROM (
			SELECT ROW_NUMBER() OVER (ORDER BY i.CollectionSpecimenID) as row_no,
				i.CollectionSpecimenID,
				i.IdentificationUnitID,
				i.IdentificationSequence,
				left(convert(varchar, i.IdentificationDate, 120),25) as IdentificationDate,
				i.IdentificationDay,
				i.IdentificationMonth,
				i.IdentificationYear,
				cast(i.VernacularTerm as varchar(255)) as VernacularTerm,
				cast(i.TaxonomicName as varchar(255)) as TaxonomicName,
				cast(i.NameURI as varchar(255)) as NameURI,
				cast(i.IdentificationCategory as varchar(50)) as IdentificationCategory,
				cast(i.IdentificationQualifier as varchar(50)) as IdentificationQualifier,
				cast(i.TypeStatus as varchar(50)) as TypeStatus,
				cast(i.TypeNotes as varchar(max)) as TypeNotes,
				cast(i.ReferenceTitle as varchar(255)) as ReferenceTitle,
				cast(i.ReferenceURI as varchar(255)) as ReferenceURI,
				cast(i.ReferenceDetails as varchar(255)) as ReferenceDetails,
				cast(i.Notes as varchar(max)) as Notes,
				cast(i.ResponsibleName as varchar(255)) as ResponsibleName,
				cast(i.ResponsibleAgentURI as varchar(255)) as ResponsibleAgentURI,
				cast(s.DataWithholdingReason as varchar(50)) as DataWithholdSpecimen,
				cast(i.RowGUID as varchar(50)) as RowGUID
			from IdentificationSequenceMax i_m
				inner join Identification i on (i_m.IdentificationSequenceMax=i.IdentificationSequence and i_m.IdentificationUnitID=i.IdentificationUnitID)
				inner join CollectionProject p on p.CollectionSpecimenID=i.CollectionSpecimenID
				left join CollectionSpecimen s on i.CollectionSpecimenID = s.CollectionSpecimenID
			where %s
			) p where p.row_no>=%i and p.row_no<=%i""" % (self.projects, start, end)

	def collection_agents(self, page=0):
		start = (page*10)+1
		end = (page+1)*10
		""" in <Project>_Data """
		return """SELECT * FROM (
			SELECT ROW_NUMBER() OVER (ORDER BY ca.CollectionSpecimenID) as row_no,
				ca.CollectionSpecimenID,
				cast(ca.CollectorsName as varchar(255)) as CollectorsName,
				cast(ca.CollectorsAgentURI as varchar(255)) as CollectorsAgentURI,
				cast(left(REPLACE(REPLACE(REPLACE(REPLACE(ca.CollectorsSequence,'-',''),' ',''),':',''),'.',''),17) as bigint) AS CollectorsSequence,
				cast(ca.CollectorsNumber as varchar(50)) as CollectorsNumber,
				cast(ca.Notes as varchar(max)) as Notes,
				cast(ca.DataWithholdingReason as varchar(255)) as DataWithholdingReason,
				cast(s.DataWithholdingReason as varchar(50)) as DataWithholdSpecimen,
				cast(ca.RowGUID as varchar(50)) as RowGUID
			from CollectionAgent ca
				inner join CollectionProject p on p.CollectionSpecimenID = ca.CollectionSpecimenID
				left join CollectionSpecimen s on ca.CollectionSpecimenID = s.CollectionSpecimenID
			where %s
			) p where p.row_no>=%i and p.row_no<=%i""" % (self.projects, start, end)

	def event(self, page=0):
		start = (page*10)+1
		end = (page+1)*10
		return """SELECT * FROM (
			SELECT ROW_NUMBER() OVER (ORDER BY e.CollectionEventID) as row_no,
			e.CollectionEventID,
			e.SeriesID,
			cast(e.CollectorsEventNumber as varchar(50)) as CollectorsEventNumber,
			left(convert(varchar, e.CollectionDate, 120),25) as CollectionDate,
			e.CollectionDay,
			e.CollectionMonth,
			e.CollectionYear,
			cast(e.CollectionDateSupplement as varchar(100)) as CollectionDateSupplement,
			cast(e.LocalityDescription as varchar(max)) as LocalityDescription,
			cast(e.HabitatDescription as varchar(max)) as HabitatDescription,
			cast(e.CollectingMethod as varchar(max)) as CollectingMethod,
			cast(e.Notes as varchar(max)) as Notes,
			cast(e.CountryCache as varchar(50)) as CountryCache,
			cast(e.DataWithholdingReason as varchar(255)) as DataWithholdingReason,
			cast(e.RowGUID as varchar(50)) as RowGUID
		from CollectionEvent e
			inner join (
				select c.CollectionEventID from CollectionSpecimen c
						inner join IdentificationUnit iu on c.CollectionSpecimenID=iu.CollectionSpecimenID
						inner join IdentificationSequenceMax i on i.IdentificationUnitID=iu.IdentificationUnitID
						inner join CollectionProject p on p.CollectionSpecimenID=c.CollectionSpecimenID
				where %s and c.CollectionEventID is not NULL
				group by c.CollectionEventID
			) as s on s.CollectionEventID=e.CollectionEventID
		) p where p.row_no>=%i and p.row_no<=%i""" % (self.projects, start, end)

	def event_localisation(self, page=0):
		start = (page*10)+1
		end = (page+1)*10
		return """SELECT * FROM (
			SELECT ROW_NUMBER() OVER (ORDER BY l.CollectionEventID) as row_no,
				l.CollectionEventID,
				l.LocalisationSystemID,
				cast(l.Location1 as varchar(255)) as Location1,
				cast(l.Location2 as varchar(255)) as Location2,
				cast(l.LocationAccuracy as varchar(50)) as LocationAccuracy,
				cast(l.LocationNotes as varchar(255)) as LocationNotes,
				left(convert(varchar, l.DeterminationDate, 120),25) as DeterminationDate,
				cast(l.DistanceToLocation as varchar(50)) as DistanceToLocation,
				cast(l.DirectionToLocation as varchar(50)) as DirectionToLocation,
				cast(l.ResponsibleName as varchar(255)) as ResponsibleName,
				cast(l.ResponsibleAgentURI as varchar(255)) as ResponsibleAgentURI,
				cast(l.Geography.Lat as float) as GeoLat,
				cast(l.Geography.Long as float) as GeoLong,
				cast(l.RowGUID as varchar(50)) as RowGUID,
				cast(l.RecordingMethod as varchar(max)) as RecordingMethod
			from CollectionEventLocalisation l
				inner join (
					select c.CollectionEventID from CollectionSpecimen c
						inner join IdentificationUnit iu on c.CollectionSpecimenID=iu.CollectionSpecimenID
						inner join IdentificationSequenceMax i on i.IdentificationUnitID=iu.IdentificationUnitID
						inner join CollectionProject p on p.CollectionSpecimenID=c.CollectionSpecimenID
					where %s and c.CollectionEventID is not NULL
					group by c.CollectionEventID
				) as s on s.CollectionEventID=l.CollectionEventID
				) p where p.row_no>=%i and p.row_no<=%i""" % (self.projects, start, end)

	def localisation_system(self, page=0):
		start = (page*10)+1
		end = (page+1)*10
		return """SELECT * FROM (
			SELECT ROW_NUMBER() OVER (ORDER BY LocalisationSystemID) as row_no,
				LocalisationSystemID,
				LocalisationSystemParentID,
				LocalisationSystemName,
				DefaultAccuracyOfLocalisation,
				DefaultMeasurementUnit,
				ParsingMethodName,
				DisplayText,
				DisplayEnable,
				DisplayOrder,
				Description,
				DisplayTextLocation1,
				DescriptionLocation1,
				DisplayTextLocation2,
				DescriptionLocation2,
				RowGUID
			FROM LocalisationSystem) p where p.row_no>=%i and p.row_no<=%i""" % (start, end)

	def specimen_image(self, page=0):
		start = (page*10)+1
		end = (page+1)*10
		return """SELECT * FROM (
			SELECT ROW_NUMBER() OVER (ORDER BY si.CollectionSpecimenID) as row_no,
				si.CollectionSpecimenID,
				cast(si.URI as varchar(255)) as URI,
				cast(si.ResourceURI as varchar(255)) as ResourceURI,
				si.SpecimenPartID,
				si.IdentificationUnitID,
				cast(si.ImageType as varchar(50)) as ImageType,
				cast(si.Notes as varchar(max)) as Notes,
				cast(w.DataWithholdingReason as varchar(50)) as DataWithholdSpecimen,
				cast(si.DataWithholdingReason as varchar(255)) as DataWithholdingReason,
				left(convert(varchar, si.LogCreatedWhen, 120),25) as LogCreatedWhen,
				cast(si.LogCreatedBy as varchar(50)) as LogCreatedBy,
				left(convert(varchar, si.LogUpdatedWhen, 120),25) as LogUpdatedWhen,
				cast(si.LogUpdatedBy as varchar(50)) as LogUpdatedBy,
				cast(si.RowGUID as varchar(50)) as RowGUID,
				cast(si.Title as varchar(500)) as Title,
				cast(si.IPR as varchar(500)) as IPR,
				cast(si.CreatorAgent as varchar(500)) as CreatorAgent,
				cast(si.CreatorAgentURI as varchar(255)) as CreatorAgentURI,
				cast(si.CopyrightStatement as varchar(500)) as CopyrightStatement,
				cast(si.LicenseType as varchar(500)) as LicenseType,
				cast(si.InternalNotes as varchar(500)) as InternalNotes,
				cast(si.LicenseHolder as varchar(500)) as LicenseHolder,
				cast(si.LicenseHolderAgentURI as varchar(500)) as LicenseHolderAgentURI,
				cast(si.LicenseYear as varchar(50)) as LicenseYear,
				si.DisplayOrder
			from CollectionSpecimenImage si inner join (
			select s.CollectionSpecimenID from CollectionSpecimen s
				inner join IdentificationUnit iu on s.CollectionSpecimenID=iu.CollectionSpecimenID
				inner join IdentificationSequenceMax i on i.IdentificationUnitID=iu.IdentificationUnitID
				inner join CollectionProject p on p.CollectionSpecimenID=s.CollectionSpecimenID
			where %s
			group by s.CollectionSpecimenID
		) as p on p.CollectionSpecimenID=si.CollectionSpecimenID
		left join CollectionSpecimen w on w.CollectionSpecimenID=si.CollectionSpecimenID
		) p where p.row_no>=%i and p.row_no<=%i""" % (self.projects, start, end)

	def specimen_relation(self, page=0):
		start = (page*10)+1
		end = (page+1)*10
		return """SELECT * FROM (
			SELECT ROW_NUMBER() OVER (ORDER BY r.CollectionSpecimenID) as row_no,
				r.[CollectionSpecimenID]
				,cast(r.[RelatedSpecimenURI] AS nvarchar(255)) as RelatedSpecimenURI
				,cast(r.[RelatedSpecimenDisplayText] AS nvarchar(255)) as RelatedSpecimenDisplayText
				,cast(r.[RelationType] AS nvarchar(50)) as RelationType
				,r.[RelatedSpecimenCollectionID]
				,cast(r.[RelatedSpecimenDescription] AS nvarchar(max)) as RelatedSpecimenDescription
				,cast(r.[Notes] AS nvarchar(max)) as Notes
				,cast(r.[RowGUID] AS nvarchar(50)) as RowGUID
				,r.[IdentificationUnitID]
				,r.[SpecimenPartID]
			FROM CollectionSpecimenRelation r inner join (
			select s.CollectionSpecimenID from CollectionSpecimen s
				inner join IdentificationUnit iu on s.CollectionSpecimenID=iu.CollectionSpecimenID
				inner join IdentificationSequenceMax i on i.IdentificationUnitID=iu.IdentificationUnitID
				inner join CollectionProject p on p.CollectionSpecimenID=s.CollectionSpecimenID
			where %s
			group by s.CollectionSpecimenID
		) as p on p.CollectionSpecimenID=r.CollectionSpecimenID
			left join CollectionSpecimen w on w.CollectionSpecimenID=r.CollectionSpecimenID
		) p where p.row_no>=%i and p.row_no<=%i""" % (self.projects, start, end)

	def identification_unit_analysis(self, page=0):
		start = (page*10)+1
		end = (page+1)*10
		return """SELECT * FROM (
			SELECT ROW_NUMBER() OVER (ORDER BY iua.CollectionSpecimenID) as row_no,
				iua.CollectionSpecimenID,
				iua.IdentificationUnitID,
				iua.AnalysisID,
				cast(iua.AnalysisNumber as varchar(50)) as AnalysisNumber,
				cast(iua.AnalysisResult as varchar(max)) as AnalysisResult,
				cast(iua.ExternalAnalysisURI as varchar(255)) as ExternalAnalysisURI,
				cast(iua.ResponsibleName as varchar(255)) as ResponsibleName,
				cast(iua.ResponsibleAgentURI as varchar(255)) as ResponsibleAgentURI,
				cast(iua.AnalysisDate as varchar(50)) as AnalysisDate,
				iua.SpecimenPartID,
				cast(iua.Notes as varchar(max)) as Notes,
				left(convert(varchar, iua.LogCreatedWhen, 120),25) as LogCreatedWhen,
				cast(iua.LogCreatedBy as varchar(50)) as LogCreatedBy,
				left(convert(varchar, iua.LogUpdatedWhen, 120),25) as LogUpdatedWhen,
				cast(iua.LogUpdatedBy as varchar(50)) as LogUpdatedBy,
				cast(iua.RowGUID as varchar(50)) as RowGUID,
				cast(w.DataWithholdingReason as varchar(50)) as DataWithholdSpecimen
			from IdentificationUnitAnalysis iua inner join (
			select s.CollectionSpecimenID from CollectionSpecimen s
				inner join IdentificationUnit iu on s.CollectionSpecimenID=iu.CollectionSpecimenID
				inner join IdentificationSequenceMax i on i.IdentificationUnitID=iu.IdentificationUnitID
				inner join CollectionProject p on p.CollectionSpecimenID=s.CollectionSpecimenID
			where %s
			group by s.CollectionSpecimenID
		) as p on p.CollectionSpecimenID=iua.CollectionSpecimenID
		left join CollectionSpecimen w on w.CollectionSpecimenID=iua.CollectionSpecimenID
		) p where p.row_no>=%i and p.row_no<=%i""" % (self.projects, start, end)

	def identification_unit_analysis_barcoding(self, page=0):
		start = (page*10)+1
		end = (page+1)*10
		return """SELECT * FROM (
			SELECT ROW_NUMBER() OVER (ORDER BY iua.CollectionSpecimenID) as row_no,
				iua.CollectionSpecimenID,
				iua.IdentificationUnitID,
				iua.AnalysisID,
				cast(iua.ToolUsage as nvarchar(max)) as ToolUsage,
				cast(w.DataWithholdingReason as varchar(50)) as DataWithholdSpecimen
			FROM IdentificationUnitAnalysis iua inner join (
			select s.CollectionSpecimenID from CollectionSpecimen s
				inner join IdentificationUnit iu on s.CollectionSpecimenID=iu.CollectionSpecimenID
				inner join IdentificationSequenceMax i on i.IdentificationUnitID=iu.IdentificationUnitID
				inner join CollectionProject p on p.CollectionSpecimenID=s.CollectionSpecimenID
			where %s
			group by s.CollectionSpecimenID
			) as p on p.CollectionSpecimenID=iua.CollectionSpecimenID
			left join CollectionSpecimen w on w.CollectionSpecimenID=iua.CollectionSpecimenID
			where ToolUsage is not null and (iua.AnalysisID=95 or iua.AnalysisID=110)
		) p where p.row_no>=%i and p.row_no<=%i""" % (self.projects, start, end)

class insert_data(ProjectDB):
	""" Insert data from DiversityCollection into BioCASe tables """
	def __init__(self, section):
		ProjectDB.__init__(self, section)
		self.not_found = {}
		self.cur = self.con.cursor()
		self.project_ids = section.get_project_ids()

	def _getColumnHeaders(self, table):
		resU = []
		U = resU.append
		q = """SELECT `COLUMN_NAME`
			FROM `INFORMATION_SCHEMA`.`COLUMNS`
			WHERE `TABLE_SCHEMA`='{0}'
				AND `TABLE_NAME`='{1}'""".format(self.new_db, table)
		self.cur.execute(q)
		for row in self.cur.fetchall():
			U(row[0])
		return resU

	def _insertQuery(self, table):
		columns = self._getColumnHeaders(table)
		resA = u"""INSERT INTO {0} ({1}) VALUES ({2})""".format(table, ",".join(columns), ",".join(['{%s}'%c for c in columns]))
		return resA

	def _deleteQuery(self, table):
		if table==None:
			return False
		elif table in ('CollectionEvent', 'CollectionEventLocalisation'):
			return False
		else:
			if table == 'LocalisationSystem':
				resA = """TRUNCATE `{0}`""".format(table)
			elif table == 'CollectionProject':
				resA = """delete p from {0} p WHERE p.ProjectID in ({1})""".format(table, ", ".join([str(i) for i in self.project_ids]))
			elif table == 'ProjectProxy':
				resA = """delete p from {0} p WHERE p.ProjectID in ({1})""".format(table, ", ".join([str(i) for i in self.project_ids]))
			else:
				resA = """delete s from {0} s left join CollectionProject p on p.CollectionSpecimenID=s.CollectionSpecimenID WHERE p.ProjectID in ({1})""".format(table, ", ".join([str(i) for i in self.project_ids]))
			return resA

	def _truncate_target(self, target=None):
		if target==None:
			return ""
		else:
			return "TRUNCATE `%s`" % (target)

	def _cleanupQuery(self, table):
		if table in ('CollectionEvent', 'CollectionEventLocalisation'):
			return """delete e from {0} e left join CollectionSpecimen s on e.CollectionEventID=s.CollectionEventID WHERE s.CollectionSpecimenID IS NULL""".format(table)
		return False

	def projects(self, data):
		self.query = u"""INSERT INTO ProjectProxy (`ProjectID`,`Project`) VALUES ({ProjectID}, "{Project}")"""
		self.cur.execute(self._deleteQuery(table='ProjectProxy'))
		self.commit()
		for entry in data.yield_items():
			self.cur.execute(self.query.format(**entry))
			self.commit()

	def base_url(self, data):
		self.query = u"""INSERT INTO BaseURL (`text`) VALUES ("{text}")"""
		self.cur.execute(self._truncate_target('BaseURL'))
		self.commit()
		for entry in data.yield_items():
			self.cur.execute(self.query.format(**entry))
			self.commit()

	def data(self, data_obj, entries=0):
		database_tables = data_obj.getAvailableDBTables()
		for table in database_tables:
			page = 0
			stop = False
			
			logging.info("Reset table `%s`" % table) 
			query = self._deleteQuery(table)
			if query:
				self.cur.execute(query)
				self.commit()
			while not stop:
				with warnings.catch_warnings():
					warnings.simplefilter('error', mdb.Warning)

					logging.info("Get data for %s, page %i:" % (table, page+1)) 
					data_obj.getData(table, page)
					query = self._insertQuery(table)
					if data_obj.length()<10:
						stop = True
					if data_obj.length()==0:
						break
					
					for entry in data_obj.yield_items():
						f_query = query.format(**entry)
						try:
							self._executeQuery(query=f_query, calls=1, debug=repr(entry)[:120].replace('"',''))
						except Exception, e:
							if e.args[0]==1062 and table in ('CollectionEvent', 'CollectionEventLocalisation'):  # -- MySQL Error [1062]: Duplicate entry
								if table=='CollectionEvent':
									self.cur.execute("DELETE FROM {0} WHERE CollectionEventID={1}".format(table, entry['CollectionEventID']))
									self.commit()
								if table=='CollectionEventLocalisation':
									self.cur.execute("DELETE FROM {0} WHERE CollectionEventID={1} and LocalisationSystemID={2}".format(table, entry['CollectionEventID'], entry['LocalisationSystemID']))
									self.commit()
								self._executeQuery(query=f_query, calls=2, debug=repr(entry)[:120].replace('"',''))
							else:
								logging.error("Error %d: %s" % (e.args[0],e.args[1])) 
				page+= 1
			c_query = self._cleanupQuery(table)
			if c_query:
				self.cur.execute(c_query)
				self.commit()
				

	def _executeQuery(self, query, calls, debug):
		try:
			self.cur.execute(query)
			self.commit()
		except mdb.Warning, e:
			logging.error("MySQL error: %r" % e) 
			logging.warning("Query: %s" % query) 
		except mdb.Error, e:
			if e.args[0]==1062 and calls<2:  # -- MySQL Error [1062]: Duplicate entry
				raise Exception, e
			else:
				try:
					logging.error("MySQL Error [%d]: %s") % (e.args[0], e.args[1]) 
				except UnicodeEncodeError:
					logging.error("Not unicode: %s" % str(e)) 
		except Exception, e:
			logging.error("Error %d: %s<br><br>" % (e.args[0],e.args[1])) 
			logging.warning("Query: %s<br><br>" % q) 
		else:
			if calls == 1:
				s = "Inserted (%s...)" % debug 
			else:
				s = "Updated (%s...)" % debug 
			logging.info(s)
