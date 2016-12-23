
f1 = open('p6.txt', 'r')
f2 = open('temp.txt', 'w')
for line in f1:
    f2.write(line.replace(' \n', ' 41\n'))
f1.close()
f2.close()
