import pgdb, re

class DBHandler:
  def __init__(self, sc, dbhost='localhost', dbname='autoschemer', dbuser='postgres', dbpwd = None):
    self.db = pgdb.connect(database=dbname, user=dbuser, host=dbhost, password=dbpwd)
    self.schema = sc;
    sc_name = sc.get_name()
    cursor = self.db.cursor()
    cursor.execute("DROP SCHEMA IF EXISTS {} CASCADE".format(sc_name))
    cursor.execute("CREATE SCHEMA {}".format(sc_name))
    print "CREATING SCHEMA: {}".format(sc_name)
    # table_order is the order to fill in tables that avoids dependency issues
    #self.table_order = self.determine_order(sc)
    self._create_tables(sc)
    self.db.commit()
    print "COMMITED"

  #def determine_order(self, sc):
  #  return []

  def _create_tables(self, sc):
    cursor = self.db.cursor()
    types = sc.get_types()
    sc_name = self.schema.get_name()
    for t in sc.get_tables():
      sql_create_tbl = ("""CREATE TABLE {}.table_{} (
          _id SERIAL PRIMARY KEY, """.format(sc_name, t.get_id()) +
          ", ".join(["col_{} {}".format(col, types[col]) for col in t.get_cols()]) + 
          (", " if len(t.get_fkeys())>0 else "") +
          ", ".join(["fk_{} int REFERENCES {}.table_{}(_id) ON DELETE CASCADE".format(i, sc_name, fk) for i, fk in enumerate(t.get_fkeys())]) + 
        ", UNIQUE (" + ", ".join(["col_{}".format(col) for col in t.get_cols()]) + ") " + 
        ");")

      cursor.execute(sql_create_tbl)
      print "CREATING TABLE: table_{}".format(t.get_id())

  def insert_row(self, rowd):
    # row_data => array of values
    cursor = self.db.cursor()
    sc_name = self.schema.get_name()
    types = self.schema.get_types()
    row = rowd
    # for i,v in enumerate(rowd):
    #   if types[i]=='VARCHAR':
    #     row.append(re.escape(v))
    #   else:
    #     row.append(v)
    # print row
    for t in self.schema.get_tables():

      col_names = ", ".join(["col_{}".format(col) for col in t.get_cols()])
      col_values = ", ".join([self.schema.parse_data_with_type(col, row[col]) for col in t.get_cols()]) 
      col_condition_check = " AND ".join(["col_{}={}".format(c, self.schema.parse_data_with_type(c, row[c])) for c in t.get_cols()])

      fkeys_names = fkeys_values = ""
      if len(t.get_fkeys()) > 0:
        fkeys_names = ", " + ", ".join(["fk_{}".format(i) for i, _  in enumerate(t.get_fkeys())])  
        
        fkeys_values = ", " + ", ".join(["(SELECT _id FROM {}.table_{} WHERE {})".format(sc_name, v, 
        " AND ".join(["col_{}={}".format(c, self.schema.parse_data_with_type(c, row[c])) for c in self.schema.get_table(v).get_cols()])) 
        for _, v in enumerate(t.get_fkeys())])  
      
      # insert only if tuple doesn't exist already
      sql_insert=("INSERT INTO {}.table_{} ".format(sc_name, t.get_id()) + 
      "(" + col_names +  fkeys_names + ") " +  
      "SELECT " +  col_values + fkeys_values + 
      "WHERE NOT EXISTS (SELECT _id from {}.table_{} where {}".format(sc_name, t.get_id(), col_condition_check)  + 
      ") ;")

      cursor.execute(sql_insert)
    print "Inserted row: {} ".format(row)
    self.db.commit()
  

