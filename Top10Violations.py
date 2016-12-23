# Here we plot a barchart of top10 violation descriptions
import pylab as plt
# Create a dataframe of the violation type frequency
violations_df = raw_df.rollup('Violation Description').count().sort('count',ascending=False)
# Create a list with the top 10 violation's frequencies
ViolationsTop10 = violations_df.select('count').rdd.flatMap(lambda y: y).collect()
ViolationsTop10 = [ViolationsTop10[1]/1000, ViolationsTop10[2]/1000, ViolationsTop10[3]/1000, ViolationsTop10[4]/1000, ViolationsTop10[5]/1000, ViolationsTop10[6]/1000, ViolationsTop10[7]/1000, ViolationsTop10[8]/1000, ViolationsTop10[9]/1000, ViolationsTop10[10]/1000]
# Plot a bar chart of top10 violations
Type = [1,2,3,4,5,6,7,8,9,10]
LABELS = ["SPEED IN ZONE", "DISOBEYED TR. DEV.", "OP. MV MOBILE", "UNINSP. VEHICLE", "SPEED OVER 55 ZONE", "UNLICENSED OPERATOR", "SIDWING TRANSPARENT", "OPERATING W/ INSURANCE", "FAILED TO STOP", "NO SEAT BELT"]
plt.bar(Type, ViolationsTop10, align='center', color='black')
plt.xlabel('Violation Description')
plt.ylabel("#Violations(k)")
plt.xticks(Type, LABELS, rotation='45')
plt.title('Top10 Violation Types')
plt.show()
