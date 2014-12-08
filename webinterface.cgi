#!/usr/bin/python
# -*- coding: utf-8 -*-

import cgi
import pyodbc
from sections import sections

cnxn = pyodbc.connect("DSN=<ODBC-Datasource>;UID=<Username>;PWD=<Password>")
cursor = cnxn.cursor()

rows = {}
for sect in sections:
	ids = [str(e['dwb_id']) for e in sections[sect]['collections']]
	cursor.execute("""select SUM(q.q) as Quantity from (
		select count(p.ProjectID) as q, p2.Project as p
			from CollectionProject cp
				left join DiversityProjects_ZFMK.dbo.Project p on cp.ProjectID=p.ProjectID
				left join DiversityProjects_ZFMK.dbo.Project p2 on p.ProjectParentID=p2.ProjectID
			where cp.ProjectID in (%s)
			group by p.ProjectID, p2.Project
		) as q""" % ",".join(ids))

	sections[sect]['sum'] = 0 # default to zero
	row = cursor.fetchone()
	for amo in row:
		if amo is not None:
			sections[sect]['sum'] = row[0]
	
print("Content-type: text/html\n")


print("""
<html>
<head>
	<title>Daten aus einer Datenbank abrufen</title>
	<style type="text/css">
	#header {
		float: left;
	}
	#login {
		float: right;
	}
	#begin {
		clear: both;
	}
	</style>
</head>
<body>
	<h1 id="header">Sektionen der DiversityWorkbench mit Anzahl der Datens&auml;tze</h1>
	<form name="Login" action="login.py"
	method="post">
	<div id="login">Anmelden: <input type="text" size="15" name="LoginUser" placeholder="Benutzername"><input type="text" size="15" name="Password" placeholder="Passwort">
	</div></form>
<div id=begin>
<table border="0">
<colgroup>
	<col width="190">
	<col width="120">
	<col width="120">
</colgroup>
<tr>
	<th>Untersektion</th>
	<th>Projektnummer</th>
	<th>Anzahl Eintr&auml;ge</th>
</tr>""")

for sect, sect_det in sorted(sections.iteritems()):
	print("""
	<tr>
		<td>
			<a href="sec_detail.cgi?section=%s">%s</a>
		</td>
		<td>%s</td>
		<td align="right">%s</td>
	</tr>""") % (sect, sect_det['name'][1]['title'], sect_det['dwb_id'], sect_det['sum'])

print("""
	<tr><td colspan="3"><center>----------</center></td></tr>
	<tr>
		<td colspan="3"><center><span style="color:red">
			To-Do: <li>Upload: Eingabe durch Benutzer, &uuml;bertragung zu Python</li>
			<li>Trennung HTML und Python</li>
			<li>JavaScript/JSON Implementierung</li>
			<li>Layout (CSS) am Ende</li></span><br><br></center>
		</td>
	</tr>""")

print("""
</table>
</div>

</body>
</html>""")
