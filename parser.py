import csv, itertools

def parse(file): 
	with open(file, 'rb') as csvfile:
		reader, reader2 = itertools.tee(csv.reader(csvfile, delimiter=' ', quotechar='|'))
		data = [set() for j in next(reader2)]
		columns = len(data)
		count = 0
		print data
		for row in reader:
			count += 1
			for j in row:
				
				data[j].add(row[j])
   	print data

parse('data.csv');
