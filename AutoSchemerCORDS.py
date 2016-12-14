from AutoSchemer import AutoSchemer
import Parser
import csv, itertools, sys, os
from SchemaObjects import Table, Schema
    
class AutoSchemerCORDS(AutoSchemer):
  def __init__(self, data_files):
    AutoSchemer.__init__(self, data_files)
    self.distinct_rows = []

  def run(self, prune=False):
    data_file = self.data_files[0]

    if prune:
      (columns, types) = Parser.parse_prune_cords(data_file)
    else:
      (data, self.distinct_rows, columns, types) = Parser.parse_cords(data_file)

    self.sc.set_types(types)
    #self.sc.print_types()
    self._create_schema(data_file, columns)
    print self.sc

  def _create_schema(self, file, columns):
    self._add_table(file, columns)

  def _add_table(self, file, columns):
    # columns => column numbers considered in the recursive loop
    with open(file, 'rb') as csvfile:
      done = set()
      primary_key = set()
      map_foreign_keys= []

      remaining = columns
      table_count = 1

      readers = itertools.tee(csv.reader(csvfile), len(columns)*len(columns)/2)
      #reader = csv.reader(csvfile)
      count = 0
      for r in readers[0]:
        count += 1

      # compare column i with the other columns
      ccc = 1 
      while (len(remaining) > 0):
        co = remaining[0]
        valid = [co]
        for i, co2 in enumerate([x for x in remaining if x != co]):
                  # first figure out distinct values in each column
            pair = set()
            #reader = csv.reader(csvfile)
            for r in readers[ccc]:
              appended = ""
              for j,v in enumerate(r):
                if (j == co or j == co2):
                  appended += r[j] + " "
              pair.add(appended)
            print len(pair), count, self.distinct_rows[i]
            if len(pair) <= count * 0.3 and self.distinct_rows[i] >= 0.8*len(pair):
              valid.append(co2)
            ccc += 1
        
        for c in valid:
          done.add(c)
          remaining.remove(c)
          # print columns, c
        id_col = str(table_count) + "_id"
        valid.append(id_col)
        t =  Table(valid, valid, [])
        self.sc.add(t)
        map_foreign_keys.append(t.get_id())
        table_count += 1

      # add remaining map table
      id_col = str(table_count) + "_id"
      t = Table([id_col], [id_col], map_foreign_keys)
      self.sc.add(t)
      return t.get_id()
