import csv, itertools, sys, os
import Parser
from DBHandler import DBHandler

class Table:
  idx = 0
  def __init__(self, cols, primary_key, foreign_keys):
    Table.idx += 1
    self.id = Table.idx
    self.cols = cols
    self.primary_keys = primary_key
    self.foreign_keys = foreign_keys

  def get_id(self):
    return self.id

  def get_cols(self):
    return self.cols

  def get_pkeys(self):
    return self.primary_keys

  def get_fkeys(self):
    return self.foreign_keys

  def __str__(self):
    output = "Table Id: {}".format(self.id) + "\n" 
    output += "Column Numbers: " + ",".join([str(x) for x in self.cols]) + "\n" 
    output += "Foreign Key Tables: " + ",".join([str(x) for x in self.foreign_keys]) + "\n"
    return output

class Schema:
  def __init__(self, schema_name):
    self.schema_name = schema_name
    self.tables = []
    self.types = []
    self.tmap = {}

  def __str__(self):
    output =  "Schema: {} \n".format(self.schema_name) + "\n".join([str(table) for table in self.tables]) 
    return output 
  
  def get_name(self):
    return self.schema_name

  def get_tables(self):
    return self.tables

  def get_table(self, i):
    return self.tmap[i]

  def parse_data_with_type(self, col, data):
    if self.types[col] == 'INT':
      return data
    if self.types[col] == 'FLOAT':
      return data
    if self.types[col] == 'VARCHAR':
      return "\'{}\'".format(data)
  
  def get_types(self):
    return self.types

  def add(self, table):
    self.tables.append(table)
    self.tmap[table.get_id()] = table

  def set_types(self, types):
    self.types = types

  def print_types(self):
    print "Type Guesses:"
    for i, tg in enumerate(tgs):
      print "Col {} : {}".format(i, tg.get_type())
  
  def create_schema(self, file):
    self.create_table(file, col_order)

  def create_table(self, file, columns_use):
    columns_use = set(columns_use)
    # columns => column numbers considered in the recursive loop
    columns = [i for i in col_order if i in columns_use]
    with open(file, 'rb') as csvfile:
      readers = itertools.tee(csv.reader(csvfile, delimiter=',', quotechar='|'), len(columns))

      done = set()
      primary_key = set()
      foreign_keys= set()
      for i, co in enumerate(columns):
        if co not in done:
          # valid => set of valid column numbers to compare against
          valid = set([c for c in columns if c not in done and c != co])
          prev = [None for _ in col_order]
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
            table_id = self.create_table(file, next_columns)
            foreign_keys.add(table_id)
          else: # if the column cannot be separated into a separate table, just add it to the primary_key/columns of the current table
            primary_key.add(co)
          done.add(co)
      #print "primary_key", primary_key

      # create new table with col = primary_key, and foreign_keys be a reference to other table_ids
      t = Table(list(primary_key), list(primary_key), list(foreign_keys))
      self.add(t)
      return t.get_id()
      

if __name__ == '__main__':
  schema_name = "testSchema"
  global col_order 
  global sc
  sc = Schema(schema_name)
  
  data_file = 'data/test.csv'
  if len(sys.argv) == 1:
    print "No data file argument found... using default file 'data/test.csv'"
  else:
    data_file = sys.argv[1]

  # distinctRows => (col_num, num_distinct_rows)
  # col_order => [col_num, col_num ...], ordered by most distinct rows
  # tgs => list of TypeGuessers for each column
  # NOTE: Don't really use distinctRows currently... keeping just in case
  (distinctRows, col_order, tgs) = Parser.parse(data_file)
  sc.set_types([tg.get_type() for tg in tgs])
  sc.print_types()
  sc.create_schema(data_file)
  #create_schema(data_file, col_order)
  print sc

  ###################
  dbname = os.environ['dbname']
  dbuser = os.environ['dbuser']
  dbhost = os.environ['dbhost']
  dbpwd = os.environ['dbpwd']
  print "Beginning to create schema and tables in psql"
  print "db_name: {}".format(dbname)
  print "db_user: {}".format(dbuser)

  dbh = DBHandler(sc, dbhost=dbhost, dbname=dbname, dbuser=dbuser, dbpwd=dbpwd)
  with open(data_file, 'rb') as csvfile:
    reader = csv.reader(csvfile, delimiter=',', quotechar='|')
    [dbh.insert_row(row) for row in reader]
