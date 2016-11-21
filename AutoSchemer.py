import csv, itertools, sys, os
import Parser
from SchemaObjects import Table, Schema
from DBHandler import DBHandler
from DBConnection import DBConnection
    
class AutoSchemer:
  def __init__(self, data_files):
    self.data_files = data_files
    schema_name = "testSchema"
    # col_order is order to traverse columns
    self.col_order = []
    self.distinct_rows = []
    self.sc = Schema(schema_name)

  def run(self):
    data_file = self.data_files[0]

    (self.distinct_rows, self.col_order, types) = Parser.parse(data_file)
    self.sc.set_types(types)
    self.sc.print_types()
    self.create_schema(data_file)
    print self.sc

  def create_schema(self, file):
    self.add_table(file, self.col_order)

  def add_table(self, file, columns_use):
    columns_use = set(columns_use)
    # columns => column numbers considered in the recursive loop
    columns = [i for i in self.col_order if i in columns_use]
    with open(file, 'rb') as csvfile:
      readers = itertools.tee(csv.reader(csvfile, delimiter=',', quotechar='|'), len(columns))

      done = set()
      primary_key = set()
      foreign_keys= set()
      for i, co in enumerate(columns):
        if co not in done:
          # valid => set of valid column numbers to compare against
          valid = set([c for c in columns if c not in done and c != co])
          prev = [None for _ in self.col_order]
          compared = False
          for r in sorted(readers[i], key=lambda row:row[co]):
            #print "prev: ", prev
            #print "r: ", r
            if prev[co] == r[co]:
              compared = True
              [valid.remove(c) for c in list(valid) if prev[c] != r[c]]
            
            # if no more valid columns to compare against break out
            if len(valid) == 0:
              break;

            # update prev
            for c in valid:
              prev[c] = r[c]
            prev[co] = r[co]
         
          # if after the for loop, valid is not empty, that means that the columns could potentially
          #   be separated into a different table. Add a few checks, making sure that the column checked
          #   didn't just only have unique values.

          if len(valid) != 0 and compared and (len(valid)!=len(columns)-1):
            #print co, valid
            next_columns = [co]
            for c in valid:
              done.add(c)
              next_columns.append(c)
            table_id = self.add_table(file, next_columns)
            foreign_keys.add(table_id)
          else: # if the column cannot be separated into a separate table, just add it to the primary_key/columns of the current table
            primary_key.add(co)
          done.add(co)
      #print "primary_key", primary_key

      # create new table with col = primary_key, and foreign_keys be a reference to other table_ids
      t = Table(list(primary_key), list(primary_key), list(foreign_keys))
      self.sc.add(t)
      return t.get_id()


  def load_db(self):
    dbname = os.environ['dbname']
    dbuser = os.environ['dbuser']
    dbhost = os.environ['dbhost']
    dbpwd = os.environ['dbpwd']
    print "Beginning to create schema and tables in psql"
    print "db_name: {}".format(dbname)
    print "db_user: {}".format(dbuser)

    db_connection = DBConnection(dbhost=dbhost, dbname=dbname, dbuser=dbuser, dbpwd=dbpwd)
    db_connection.get_instance() 

    dbh = DBHandler(self.sc, db=db_connection.get_instance())
    for data_file in self.data_files:
      dbh.load_file(data_file)


if __name__ == '__main__':
  data_file = 'data/test.csv'
  if len(sys.argv) == 1:
    print "No data file argument found... using default file 'data/test.csv'"
  else:
    data_file = sys.argv[1]
  
  data_files = [data_file]
  Auto = AutoSchemer(data_files)
  Auto.run()
  Auto.load_db() 
