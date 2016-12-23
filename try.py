with open('p6.txt') as fhi, open('test.txt', 'w') as fho:
  for line in fhi:
    for i in line.split():
      f = float(i) 
      fho.write("%f"%f)
      fho.write(" ")
    fho.write("\n")
