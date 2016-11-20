import csv

def parse(file): 
  with open(file, 'rb') as csvfile:
    reader = csv.reader(csvfile, delimiter=' ', quotechar='|')
    data = []
    count = 0
    for row in reader:
      count += 1
      array = row[0].split(',')
      for j in range(len(array)):
        if (len(data) <= j):
          data.append([])
        data[j].append(array[j])
    return data

parse('data/test.csv');

