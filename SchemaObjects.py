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

  # TODO: fix this... hacked right now
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
    for i, t in enumerate(self.types):
      print "Col {} : {}".format(i, t)
    print ""

