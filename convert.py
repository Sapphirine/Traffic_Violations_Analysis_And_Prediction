for y in range(0,10):
        x = str(y)
        f1 = open('part-0000' +x,'r')
        f2 = open('part2-0000'+x,'w')
        for line in f1:
                f2.write(line.replace("u'", ""))
        f1.close()
        f2.close()
for y in range(0,10):
        x = str(y)
        f1 = open('part2-0000' +x,'r')
        f2 = open('part3-0000'+x,'w')
        for line in f1:
                f2.write(line.replace("'", ""))
        f1.close()
        f2.close()
for y in range(0,10):
        x = str(y)
        f1 = open('part3-0000' +x,'r')
        f2 = open('part4-0000'+x,'w')
        for line in f1:
                f2.write(line.replace("(", ""))
        f1.close()
        f2.close()
for y in range(0,10):
        x = str(y)
        f1 = open('part4-0000' +x,'r')
        f2 = open('part5-0000'+x,'w')
        for line in f1:
                f2.write(line.replace(")", ""))
        f1.close()
        f2.close()
for y in range(0,10):
        x = str(y)
        f1 = open('part5-0000' +x,'r')
        f2 = open('part6-0000'+x,'w')
        for line in f1:
                f2.write(line.replace(",", ""))
        f1.close()
        f2.close()

for y in range(10,94):
	x = str(y)
        f1 = open('part-000' +x,'r')
        f2 = open('part2-000'+x,'w')
        for line in f1:
                f2.write(line.replace("u'", ""))
        f1.close()
        f2.close()
for y in range(10,94):
        x = str(y)
        f1 = open('part2-000' +x,'r')
        f2 = open('part3-000'+x,'w')
        for line in f1:
                f2.write(line.replace("'", ""))
        f1.close()
        f2.close()
for y in range(10,94):
        x = str(y)
        f1 = open('part3-000' +x,'r')
        f2 = open('part4-000'+x,'w')
        for line in f1:
                f2.write(line.replace("(", ""))
        f1.close()
        f2.close()
for y in range(10,94):
        x = str(y)
        f1 = open('part4-000' +x,'r')
        f2 = open('part5-000'+x,'w')
        for line in f1:
                f2.write(line.replace(")", ""))
        f1.close()
        f2.close()
for y in range(10,94):
        x = str(y)
        f1 = open('part5-000' +x,'r')
        f2 = open('part6-000'+x,'w')
        for line in f1:
                f2.write(line.replace(",", ""))
        f1.close()
        f2.close()

