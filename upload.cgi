#!/usr/bin/python
# -*- coding: utf-8 -*-

import cgi
import pyodbc
from sections import sections

form = cgi.FieldStorage()
id_input = []
for i in form.keys():
	id_input.append(form[i].value)

cnxn = pyodbc.connect("DSN=<ODBC-Datasource>;UID=<Username>;PWD=<Password>")
cursor = cnxn.cursor()
cursor.execute("""select cp.ProjectID, p.Project, p.ProjectParentID, COUNT(cp.ProjectID) AS Anzahl,
               pp.Project, GETDATE() AS Datum
               from CollectionProject cp
               left join ProjectProxy pp ON pp.ProjectID=cp.ProjectID
               left join DiversityProjects_ZFMK.dbo.Project p on pp.ProjectID=p.ProjectID
               where cp.ProjectID in (%s)
               group by cp.ProjectID, p.Project, pp.Project, p.ProjectParentID
               ORDER BY pp.Project""" % ",".join(id_input))

rows_det = cursor.fetchall()

name_num_det = {}
for row in rows_det:
	name_num_det.update([(row.Project, row.Anzahl)])

print("Content-type: text/html\n")
print("""
<html>
<head>
	<title>Daten zu BioCASe hochladen</title>
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
	#lock {
		float: left;
		width: 100px;
	}
	#fields {
		margin-left: 100px;
	}
	</style>
	<script src="http://code.jquery.com/jquery-1.11.0.min.js"></script>
	<script type="text/javascript">
	function checkForm(form) {
		$("#submit_field").hide();
		$("#messages").html('<p>Uploading - please wait...</p><img src="images/uploading.gif"/>');
		return true;
	}
	</script>
</head>
<body>
	<h1 id="header">Datens&auml;tze f&uuml;r die Untersektion %s hochladen</h1>
	<form name="Login" action="login.py" 
	method="post">
	<div id="login">Anmelden: <input type="text" size="15" name="LoginUser" placeholder="Benutzername"><input type="text" size="15" name="Password" placeholder="Passwort">
	</div></form>
	<p id="begin">Untersektion: %s<br>Eintr&auml;ge: %s""") % (str(name_num_det.keys()).replace("[",'').replace("]",'').replace("u'",'').replace("'",''), str(name_num_det.keys()).replace("[",'').replace("]",'').replace("u'",'').replace("'",''), str(name_num_det.values()).replace("[",'').replace("]",'').replace(",",' +'))
	#% (str(form.keys()).replace("[",'').replace("]",'').replace("'",'').replace(",",' &'), str(form.keys()).replace("[",'').replace("]",'').replace("'",'').replace(",",' &'), sum(list(map(int, nums))))
	#% (str(name_id_det.keys()).replace("[u'",'').replace("']",''), str(name_id_det.keys()).replace("[u'",'').replace("']",''), str(name_num_det.values()).replace("[",'').replace("]",''))

print("""
<form name="Input" action="transfer_data.cgi?""")

counter = 0
for i in id_input:
	print("""sections%s=%s&""") % (counter, i)
	counter += 1	
	
print("""" method="post" onsubmit="return checkForm(this);">
<div id="lock">Sperren:</div>
<p id="fields">Namen: <br>
<input type="checkbox" name="Collector" value="col"> Sammler<br>
<input type="checkbox" name="Identifier" value="iden"> Identifizierer<br><br>
Locations: <br>
<input type="checkbox" name="Geokoord" value="geoc"> Geokoordinaten<br>
<input type="checkbox" name="Country" value="coun"> Land<br>
<input type="checkbox" name="Localdes" value="lodes"> Fundort Beschreibung<br><br></p>
Hinweis: Ganze Datens&auml;tze werden in der DiversityWorkbench &uuml;ber Withhold gesperrt.<br><br>
<div id="submit_field">
<input type="submit" value="Absenden"><input type="reset" value="Reset"><input type="button" value="Zur&uuml;ck" name="back_button" onClick="javascript:history.back(1)">
</div>
<div id="messages"><div>
</form>
</pre>
""")

print("""

</body>
</html>""")
