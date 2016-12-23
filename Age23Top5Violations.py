# Here we want to plot the top5 violation types for the age of 23 years old
import pylab as plt # Import plotting library
# Create the age 23 dataframe
age23_df = raw_df.filter(raw_df['Age at Violation'] == '23')
# Create the age 23 frequency of violarion type dataframe from high to low
age23_df.rollup('Violation Description').count().sort('count',ascending=False).show()
# Create the top5 violation type dataframe from high to low
age23_Top5 = age23_df.rollup('Violation Description').count().sort('count',ascending=False)
# Create a list with top5 violation tyoes frequency per 1000 of age 23
age23_violations = age23_Top5.select('count').rdd.flatMap(lambda y: y).collect()
age23_violations = [age23_violations[1]/1000, age23_violations[2]/1000, age23_violations[3]/1000, age23_violations[4]/1000, age23_violations[5]/1000]
# Plot the top5 violation types for the Age of 23
LABELS = ["SPEEDING", "UNLICENSED", "SIDEWING TRANSP.", "DISOBEYED TR. DEV.", "SPEED OVER 55"]
Type = [1,2,3,4,5]
plt.bar(Type, age23_violations, align='center', color='purple')
plt.title('Age 23 Top5 Violation Types')
plt.xlabel('Violation Description')
plt.ylabel("#Violations(k)")
plt.xticks(Type, LABELS, rotation='45')
plt.show()
