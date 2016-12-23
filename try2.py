import csv  
  
in_file = "/home/srinidhi/.linuxbrew/Cellar/spark-2.0.1-bin-hadoop2.7/p5.csv"  
out_file = "t3.csv"  
  
row_reader = csv.reader(open(in_file, "rb"))  
row_writer = csv.writer(open(out_file, "wb"))  
  
first_row = row_reader.next()  
row_writer.writerow(first_row)  
for row in row_reader:  
    new_row = [val if val else "41" for val in row] + (["41"] * (len(first_row) - len(row)))  
#    print row, "->", new_row  
    row_writer.writerow(new_row)  
