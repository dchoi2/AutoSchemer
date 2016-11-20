import csv, itertools

def parse(file): 
  data = []
  with open(file, 'rb') as csvfile:
    reader, reader2 = itertools.tee(csv.reader(csvfile, delimiter=',', quotechar='|'))
    data = [set() for j in next(reader)]
    columns = len(data)
    count = 0
    for row in reader2:
      count += 1
      for j, v in enumerate(row):
        data[j].add(row[j])
  distinctRows = [(i,len(x)) for i,x in enumerate(data)]
  col_order = [i for i,v in sorted(distinctRows, key=lambda v: v[1], reverse=True)]
  return (distinctRows, col_order)
  
