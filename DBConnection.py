import pgdb

class DBConnection:
  def __init__(self, dbhost='localhost', dbname='autoschemer', dbuser='postgres'):
    self.dbhost = dbhost
    self.dbname = dbname
    self.dbuser = dbuser
    self.db = None
