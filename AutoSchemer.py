import csv, itertools, sys, os
from SchemaObjects import Table, Schema
from DBHandler import DBHandler
from DBConnection import DBConnection

class AutoSchemer:
  def __init__(self, data_files):
    self.data_files = data_files
    schema_name = "testSchema"
    self.sc = Schema(schema_name)

  def load_db(self):
    dbname = os.environ['dbname']
    dbuser = os.environ['dbuser']
    dbhost = os.environ['dbhost']
    dbpwd = os.environ['dbpwd']
    print "---LOADING CSV FILES TO DATABASE---"
    print "Using credentials for db: {}, user: {} ".format(dbname, dbuser)
    
    try:
      db_connection = DBConnection(dbhost=dbhost, dbname=dbname, dbuser=dbuser, dbpwd=dbpwd)
    except Exception as e:
      print "Database Connection Failed: {}".format(e)
      return
    print "Database Connection Successful"
    dbh = DBHandler(self.sc, db=db_connection.get_instance())
    for data_file in self.data_files:
      dbh.load_file(data_file)