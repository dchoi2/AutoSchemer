import csv, itertools, sys
import Parser

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

  def __str__(self):
    output = "Table Id: {}".format(self.id) + "\n" 
    output += "Column Numbers: " + ",".join([str(x) for x in self.cols]) + "\n" 
    output += "Foreign Key Tables: " + ",".join([str(x) for x in self.foreign_keys]) + "\n"
    return output

def create_schema(file, columns_use):
  #print "CREATING SCHEMA" 
  columns_use = set(columns_use)
  columns = [i for i in col_order if i in columns_use]
  with open(file, 'rb') as csvfile:
    readers = itertools.tee(csv.reader(csvfile, delimiter=',', quotechar='|'), len(columns))

    done = set()
    primary_key = set()
    foreign_keys= set()
    for i, co in enumerate(columns):
      if co not in done:
        #print "co: ", co
        valid = set([c for c in columns if c not in done and c != co])
        #print "valid: ", valid
        prev = [None for _ in col_order]
        compared = False
        for r in sorted(readers[i], key=lambda row:row[co]):
          #print "prev: ", prev
          #print "r: ", r
          if prev[co] == r[co]:
            compared = True
            for c in [x for x in list(valid)]:
              if prev[c] != r[c]:
                valid.remove(c)
          if len(valid) == 0:
            break;

          # update prev
          for c in valid:
            prev[c] = r[c]
          prev[co] = r[co]
        
        if len(valid) != 0 and compared and (len(valid)!=len(columns)-1):
          #print co, valid
          next_columns = [co]
          for c in valid:
            done.add(c)
            next_columns.append(c)
          table_id = create_schema(file, next_columns)
          foreign_keys.add(table_id)
        else:
          primary_key.add(co)
        # done with column co
        done.add(co)
    #print "primary_key", primary_key
    t = Table(list(primary_key), list(primary_key), list(foreign_keys))
    global tables
    tables.append(t)
    return t.get_id()
    

if __name__ == '__main__':
  global distinctRows
  global col_order 
  
  data_file = 'data/test.csv'
  if len(sys.argv) == 1:
    print "No data file argument found... using default file 'data/test.csv'"
  else:
    data_file = sys.argv[1]

  (distinctRows, col_order) = Parser.parse(data_file)

  global tables
  tables = []

  create_schema(data_file, col_order)

  print "Schema: " 
  for table in tables:
    print table
  
