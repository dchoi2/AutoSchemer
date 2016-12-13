from AutoSchemer import AutoSchemer
import Parser
import csv, itertools, sys, os
from SchemaObjects import Table, Schema
    
class AutoSchemerCORDS(AutoSchemer):
  def __init__(self, data_files):
    AutoSchemer.__init__(self, data_files)

  def run(self, prune=False):
    data_file = self.data_files[0]

    if prune:
      (self.distinct_rows, self.col_order, types) = Parser.parse_prune_simple(data_file)
    else:
      (self.distinct_rows, self.col_order, types) = Parser.parse_simple(data_file)

    self.sc.set_types(types)
    self.sc.print_types()
    self._create_schema(data_file)
    print self.sc

  def _create_schema(self, file):
    self._add_table(file, self.col_order)

  def _add_table(self, file, columns_use):
    columns_use = set(columns_use)
    # columns => column numbers considered in the recursive loop
    columns = [i for i in self.col_order if i in columns_use]
    with open(file, 'rb') as csvfile:
      readers = itertools.tee(csv.reader(csvfile, delimiter=',', quotechar='|'), len(columns))

      done = set()
      primary_key = set()
      foreign_keys= set()

      # compare column i with the other columns
      for i, co in  enumerate(columns):
        if co not in done:
          # valid => set of valid column numbers to compare against
          valid = set([c for c in columns if c not in done and c != co])
          prev = [None for _ in self.col_order]
          compared = False

          #similar stuff
          similarity_test =  set([c for c in valid if c != co])
          similarity = []
          for v in similarity_test:
            similarity.append(set())

          row_count = 0
          for r in sorted(readers[i], key=lambda row:row[co]):
            #print "prev: ", prev
            #print "r: ", r
            row_count+=1;
            if prev[co] == r[co]:
              compared = True
              [valid.remove(c) for c in list(valid) if prev[c] != r[c]]
            
            # if no more valid columns to compare against break out
            # if len(valid) == 0:
            #   break;

            for i,c in enumerate(similarity_test):
              similarity[i].add(r[co] + " " + r[c])
              prev[c] = r[c]
            prev[co] = r[co]

            # update prev
            for c in similarity_test:
              prev[c] = r[c]
            prev[co] = r[co]    
          
          for i,c in enumerate(similarity_test):    
            if (len(similarity[i]) <= 0.8 * row_count):
              valid.add(c); #add it back
         
          # if after the for loop, valid is not empty, that means that the columns could potentially
          #   be separated into a different table. Add a few checks, making sure that the column checked
          #   didn't just only have unique values.

          if len(valid) != 0 and compared and (len(valid)!=len(columns)-1):
            #print co, valid
            next_columns = [co]
            for c in valid:
              done.add(c)
              next_columns.append(c)
            table_id = self._add_table(file, next_columns)
            foreign_keys.add(table_id)
          else: # if the column cannot be separated into a separate table, just add it to the primary_key/columns of the current table
            primary_key.add(co)
          done.add(co)
      #print "primary_key", primary_key

      # create new table with col = primary_key, and foreign_keys be a reference to other table_ids
      t = Table(list(primary_key), list(primary_key), list(foreign_keys))
      self.sc.add(t)
      return t.get_id()