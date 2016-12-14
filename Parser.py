import AutoSchemer 
import csv, itertools
import re
from random import randint
#class Types:
#  num_types = 4
#  INT, FLOAT, STRING, DATE = range(num_types)

def enum(*sequential, **named):
    enums = dict(zip(sequential, range(len(sequential))), **named)
    reverse = dict((value, key) for key, value in enums.iteritems())
    enums['reverse_mapping'] = reverse
    enums['num_types'] = len(reverse)
    return type('Enum', (), enums)

# TypeGuesser uses histogram heuristics to guess the type of each column
class TypeGuesser(object):
  Types = enum('INT', 'FLOAT', 'VARCHAR', 'DATE')
  def __init__(self):
    self.count = [0 for _ in range(self.Types.num_types)]

  # can improve this metric
  def add(self, val):
    try:
      float(val)
      self.count[self.Types.FLOAT]+=1
    except ValueError:
      try:
        int(val)
        self.count[self.Types.INT]+=1
      except ValueError:
        self.count[self.Types.VARCHAR] += 1

  def get_type(self):
      t = max(xrange(len(self.count)), key=self.count.__getitem__)
      return self.Types.reverse_mapping[t]

def parse_simple(file): 
  data = []
  with open(file, 'rb') as csvfile:
    reader, reader2 = itertools.tee(csv.reader(csvfile))
    data = [set() for _ in next(reader)]
    columns = range(len(data))
    tgs = [TypeGuesser() for _ in columns]
    count = 0
    for row in reader2:
      count += 1
      for j, v in enumerate(row):
        #de = re.escape(row[j])
        de = row[j]
        data[j].add(de)
        tgs[j].add(de)
        
  distinctRows = [(i,len(x)) for i,x in enumerate(data)]
  col_order = [i for i,v in sorted(distinctRows, key=lambda v: v[1], reverse=True)]

  types = [tg.get_type() for tg in tgs]
  return (distinctRows, col_order, types)

def parse_prune_simple(file, threshold): 
  data = []
  count = 0
  with open(file, 'rb') as csvfile:
    reader, reader2 = itertools.tee(csv.reader(csvfile))
    data = [set() for _ in next(reader)]
    columns = range(0,len(data))
    tgs = [TypeGuesser() for _ in columns]
    #print len(tgs)
    for row in reader2:
      count += 1
      for j, v in enumerate(row):
        #de = re.escape(row[j])
        #print j,v
        de = row[j]
        data[j].add(de)
        tgs[j].add(de)
  
  distinctRows = []
  separate_columns = []
  for i,x in enumerate(data):
    print i, len(x), count
    if len(x) < threshold * count:
      distinctRows.append((i, len(x)))
    else:
      separate_columns.append(i)
      
  #distinctRows = [(i,len(x)) for i,x in enumerate(data)]
  col_order = [i for i,v in sorted(distinctRows, key=lambda v: v[1], reverse=True)]

  types = [tg.get_type() for tg in tgs]
  return (distinctRows, col_order, types, separate_columns)

def parse_cords(file): 
  k = 5000
  data = []
  sampled_data = []
  with open(file, 'rb') as csvfile:
    reader, reader2 = itertools.tee(csv.reader(csvfile))
    data = [set() for _ in next(reader)]
    sampled_data = [[] for j in range(min(len(data), k))]
    k = len(sampled_data)
    columns = range(len(data))
    tgs = [TypeGuesser() for _ in columns]
    count = 0
    for row in reader2:
      update = False
      index = 0
      count += 1;

      if (count < k - 1):
        index = count;
        update = True
      else:
        if (randint(0,count-1) < k):
          # replace with k/count probability
          update = True
          index = randint(0, k-1)

      if (update):
        sampled_data[index] = []
        for j, v in enumerate(row):
          #de = re.escape(row[j])
          de = row[j]
          tgs[j].add(de)
          sampled_data[index].append(de)

  # calculate data after figuring out waht sampled data is
  data = [set() for i in sampled_data]
  for j in sampled_data:
    for col, celldata in enumerate(j):
      data[col].add(celldata)
  distinctRows = [len(x) for i, x in enumerate(data)]
        
  types = [tg.get_type() for tg in tgs]
  return (sampled_data, distinctRows, columns, types)

def parse_prune_cords(file): 
  data = []
  with open(file, 'rb') as csvfile:
    reader, reader2 = itertools.tee(csv.reader(csvfile, delimiter=',', quotechar='|'))
    data = [set() for _ in next(reader)]
    columns = range(len(data))
    tgs = [TypeGuesser() for _ in columns]
    count = 0
    for row in reader2:
      count += 1
      for j, v in enumerate(row):
        #de = re.escape(row[j])
        de = row[j]
        data[j].add(de)
        tgs[j].add(de)
        
  distinctRows = [(i,len(x)) for i,x in enumerate(data)]
  col_order = [i for i,v in sorted(distinctRows, key=lambda v: v[1], reverse=True)]

  types = [tg.get_type() for tg in tgs]
  return (distinctRows, types)
