import csv, itertools

def parse(file): 
  with open(file, 'rb') as csvfile:
    reader, reader2 = itertools.tee(csv.reader(csvfile, delimiter=',', quotechar='|'))
    #for i in reader:
    #  print i
    data = [set() for j in reader2]
    columns = len(data)
    count = 0
    print data
    for row in reader:
      count += 1
      for j, v in enumerate(row):
        data[j].add(row[j])
    print data

parse('data/test.csv');
