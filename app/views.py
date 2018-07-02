from app import app
from flask import Flask, render_template,request
import sqlalchemy as sa
from flaskext.mysql import MySQL
from sqlalchemy.orm import sessionmaker
import os
import sys
from flask_sqlalchemy import SQLAlchemy
import psycopg2
mysql = MySQL()
DB = "dev"
USER = "insight"
PWD = "Insight2018"
HOST = "insight.cxlg3frcajlc.us-east-1.redshift.amazonaws.com"
PORT = "5439"
app.config['MYSQL_DATABASE_USER'] = 'root'
app.config['MYSQL_DATABASE_PASSWORD'] = 'insight'
app.config['MYSQL_DATABASE_DB'] = 'webdata'
app.config['MYSQL_DATABASE_HOST'] = '54.208.116.206'
mysql.init_app(app)
conn = mysql.connect()
sqlcursor = conn.cursor()
con=psycopg2.connect(dbname= DB, host='insight.cxlg3frcajlc.us-east-1.redshift.amazonaws.com', 
port= '5439', user= 'insight', password= 'Insight2018')
redshiftcursor = con.cursor()

@app.route('/')
@app.route('/index')
def index():
	user = {'nickname':'Miguel'}
	return render_template("index.html",title = 'Home')

@app.route('/search')
def search():
	return render_template("search.html")

@app.route("/search",methods=['POST'])
def search_post():
	response = []
	url = request.form["url"]
	query = "select * from data where site_url = '%s'" %(url)
    	redshiftcursor.execute(query)
	redshift_result = redshiftcursor.fetchall()
	if redshiftcursor.rowcount==0:
		pass
	else:
		for i in redshift_result:
			print(i)
			response.append(i)
	json_response = [{"site_url": x[0],"content_length":x[1],"version_date": x[2],"s3_link": x[3],"zip_file":x[4]} for x in response]
	return render_template("result.html",output=json_response,db="Redshift")
