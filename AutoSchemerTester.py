import csv, itertools, sys, os
import AutoSchemer
import AutoSchemerSimple 
import AutoSchemerCORDS
from SchemaObjects import Table, Schema
from DBHandler import DBHandler
from DBConnection import DBConnection

if __name__ == '__main__':
  data_file = 'data/test.csv'
  mode = 'cords'
  prune = False;
  prune_threshold = .20
  if len(sys.argv) == 1:
    print "No data file argument found... using default file 'data/test.csv'"
  else:
    data_file = sys.argv[1]
  
  if len(sys.argv) == 3:
    mode = sys.argv[2]
    
  data_files = [data_file]

  if mode == 'simple': 
    Auto = AutoSchemerSimple.AutoSchemerSimple(data_files, prune_threshold)
    Auto.run(False)
  elif mode == 'simpleprune':
    Auto = AutoSchemerSimple.AutoSchemerSimple(data_files, prune_threshold)
    Auto.run(True)
  elif mode == 'cords':
    Auto = AutoSchemerCORDS.AutoSchemerCORDS(data_files)
    Auto.run(False)
  elif mode =='cordsprune':
    Auto = AutoSchemerCORDS.AutoSchemerCORDS(data_files)
    Auto.run(False)
  else:
    # do simple
    Auto = AutoSchemerSimple.AutoSchemerSimple(data_files)
    Auto.run(False)

  #Auto.load_db() 
