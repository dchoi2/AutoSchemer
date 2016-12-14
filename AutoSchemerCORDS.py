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
      (self.distinct_rows, columns, types) = Parser.parse_cords(data_file)

    self.sc.set_types(types)
    self.sc.print_types()
    self._create_schema(data_file, columns)
    print self.sc

  def _create_schema(self, file, columns):
    self._add_table(file, columns)

  def _add_table(self, file, columns):
    # columns => column numbers considered in the recursive loop
    with open(file, 'rb') as csvfile:
      reader, reader2 = itertools.tee(csv.reader(csvfile),2)

      done = set()
      primary_key = set()
      foreign_keys= set()

      row_count = 0;

      remaining = columns

      # compare column i with the other columns
      while (len(remaining) > 0):
        co = remaining[0]
        valid = []
        for j, co2 in enumerate([x for x in remaining if x != co]):
                  # first figure out distinct values in each column
            for r in reader2:
              appended = ""
              for j,v in enumerate(r):
                if (j == co or j == co2):
                  appended += r[j] + " "
              valid.append(appended)
            if len(valid) <= row_count * 0.5 and len(self.distinct_rows(i)) >= 0.8*len(valid):
              valid.append(co2)
        
        if len(valid) != 0:
          next_columns = [co]
          for c in valid:
            done.add(c)
            # remaining.remove(c)
            # print columns, c
            next_columns.append(c)
          foreign_keys.add(table_id)
        else: # if the column cannot be separated into a separate table, just add it to the primary_key/columns of the current table
          primary_key.add(co)
        done.add(co)
        # remaining.remove(co)


      # create new table with col = primary_key, and foreign_keys be a reference to other table_ids
      t = Table(list(primary_key), list(primary_key), list(foreign_keys))
      self.sc.add(t)
      return t.get_id()