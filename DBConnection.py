import pgdb

class DBConnection:
  def __init__(self, dbhost='localhost', dbname='autoschemer', dbuser='postgres', dbpwd=None):
    self.dbhost = dbhost
    self.dbname = dbname
    self.dbuser = dbuser
    self.db = pgdb.connect(database=dbname, user=dbuser, host=dbhost, password=dbpwd)
  
  def get_instance(self):
    return self.db
