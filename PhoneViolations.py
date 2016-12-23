# Here we plot the number of operating mobile phone while driving violations
import pylab as plt # Import plotting library
# Create dataframe for mobile violations
intoxicated_df = raw_df.filter(raw_df['Violation Description'] == 'DRIVING WHILE INTOXICATED').rollup('Age at Violation').count().sort('Age at Violation',ascending=True)
# Count the number of violations for each year
phone_df = raw_df.filter(raw_df['Violation Description'] == 'OPERATING MV MOBILE PHONE').rollup('Age at Violation').count().sort('Age at Violation',ascending=True)
phone12_df = raw_df.filter((raw_df['Violation Description'] == 'OPERATING MV MOBILE PHONE') & (raw_df['Violation Year'] == 2012)).rollup('Age at Violation').count().sort('Age at Violation',ascending=True)
phone13_df = raw_df.filter((raw_df['Violation Description'] == 'OPERATING MV MOBILE PHONE') & (raw_df['Violation Year'] == 2013)).rollup('Age at Violation').count().sort('Age at Violation',ascending=True)
phone14_df = raw_df.filter((raw_df['Violation Description'] == 'OPERATING MV MOBILE PHONE') & (raw_df['Violation Year'] == 2014)).rollup('Age at Violation').count().sort('Age at Violation',ascending=True)
phone15_df = raw_df.filter((raw_df['Violation Description'] == 'OPERATING MV MOBILE PHONE') & (raw_df['Violation Year'] == 2015)).rollup('Age at Violation').count().sort('Age at Violation',ascending=True)
# Create lists for all ages per year
phone_violations = phone_df.select('count').rdd.flatMap(lambda y: y).collect()
del phone_violations[0]
del phone_violations[0]
phone12_violations = phone12_df.select('count').rdd.flatMap(lambda w: w).collect()
del phone12_violations[0]
del phone12_violations[0]
phone12_violations.extend([0, 0])
phone13_violations = phone13_df.select('count').rdd.flatMap(lambda x: x).collect()
del phone13_violations[0]
del phone13_violations[0]
phone13_violations.extend([0])
phone14_violations = phone14_df.select('count').rdd.flatMap(lambda y: y).collect()
del phone14_violations[0]
del phone14_violations[0]
phone15_violations = phone15_df.select('count').rdd.flatMap(lambda z: z).collect()
del phone15_violations[0]
del phone15_violations[0]
phone15_violations.extend([0])
age_list = [16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95]
# Plot #Violations vs Age
plt.title('PhoneViolations')
plt.plot(age_list, phone_violations)
plt.ylabel("#Violations")
plt.xlabel("Age")
plt.show()
# Plot violations through the years
labels = ['2012', '2013', '2014', '2015']
plt.title('Mobile Through the Years')
plt.ylabel("#Violations")
plt.xlabel("Age")
plt.plot(age_list, phone12_violations)
plt.plot(age_list, phone13_violations)
plt.plot(age_list, phone14_violations)
plt.plot(age_list, phone15_violations)
plt.legend(labels, loc="best")
