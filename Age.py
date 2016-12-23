# We want to plot Age sets vs #Violations
import pylab as plt # Import plotting library
# Create the age sets
age16_25 = raw_df.filter(((raw_df['Age at Violation'] >= '16') & (raw_df['Age at Violation'] <='25'))).count()
age26_35 = raw_df.filter(((raw_df['Age at Violation'] >= '26') & (raw_df['Age at Violation'] <='35'))).count()
age36_45 = raw_df.filter(((raw_df['Age at Violation'] >= '36') & (raw_df['Age at Violation'] <='45'))).count()
age46_55 = raw_df.filter(((raw_df['Age at Violation'] >= '46') & (raw_df['Age at Violation'] <='55'))).count()
age56_65 = raw_df.filter(((raw_df['Age at Violation'] >= '56') & (raw_df['Age at Violation'] <='65'))).count()
age66_75 = raw_df.filter(((raw_df['Age at Violation'] >= '66') & (raw_df['Age at Violation'] <='75'))).count()
# Plot
AgeCall = [1,2,3,4,5,6]
Violations = [age16_25*100/14224867, age26_35*100/14224867, age36_45*100/14224867, age46_55*100/14224867, age56_65*100/14224867, age66_75*100/14224867]
LABELS = ["16-25", "26-35", "36-45", "46-55", "56-65", "66-75"]
plt.bar(AgeCall, Violations, align='center', color='purple')
plt.xticks(AgeCall, LABELS)
plt.xlabel('Age')
plt.ylabel("%Violations")
plt.title('Age VS Violations')
plt.show()
# Now we want to plot distinct values of age vs #Violations
# We calculate the frequency of each age and sort from high to low
age_df = raw_df.rollup('Age at Violation').count().sort('Age at Violation',ascending=True)
age_df.select('Age at Violation').show(82)
age_df.select('count').show(82)
# We create a list that includes #violations for ages 16-95 for the y axis and represent them per 1000
age_violations = age_df.select('count').rdd.flatMap(lambda y: y).collect()
del age_violations[0]
del age_violations[0]
age_violations_k = [x / 1000 for x in age_violations]
# We create a list hat includes ages 16-95 for the x axis
age_list = [16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95]
# We plot x vs y
plt.title('#Violations VS Ages')
plt.plot(age_list, age_violations_k,color='purple')
plt.ylabel("#Violations(k)")
plt.xlabel("Age")
plt.show()

