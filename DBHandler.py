import pgdb

class DBHandler:
  def __init__(self, sc, dbhost='localhost', dbname='autoschemer', dbuser='postgres', dbpwd = None):
    self.db = pgdb.connect(database=dbname, user=dbuser, host=dbhost, password=dbpwd)
    self.schema_name = sc.schema_name
    cursor = self.db.cursor()
    cursor.execute("DROP SCHEMA IF EXISTS {} CASCADE".format(sc.schema_name))
    cursor.execute("CREATE SCHEMA {}".format(sc.schema_name))
    print "CREATING SCHEMA: {}".format(sc.schema_name)
    # table_order is the order to fill in tables that avoids dependency issues
    #self.table_order = self.determine_order(sc)
    self._create_tables(sc)
    self.db.commit()
    print "COMMITED"

  #def determine_order(self, sc):
  #  return []

  def _create_tables(self, sc):
    cursor = self.db.cursor()
    for table in sc.tables:
      sql_create_tbl = ("""CREATE TABLE {}.table_{} (
          _id SERIAL PRIMARY KEY, """.format(sc.schema_name, table.get_id()) +
          ", ".join(["col_{} {}".format(col, sc.types[col]) for col in table.cols]) + 
          (", " if len(table.foreign_keys)>0 else "") +
          ", ".join(["fk_{} int REFERENCES {}.table_{}(_id) ON DELETE CASCADE".format(i, self.schema_name, fk) for i, fk in enumerate(table.foreign_keys)]) + 
        ");")

      cursor.execute(sql_create_tbl)
      print "CREATING TABLE: table_{}".format(table.get_id())

  #def insert_row(self, row_data):
    # row_data => array of values
    #for i, v in enumerate(row_data):
      
