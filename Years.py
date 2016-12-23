# Here we plot a barchart of the year VS #VIOLATIONS
import pylab as plt
# Count #violations per year
year_2012 = raw_df.filter(raw_df['Violation Year'] == '2012').count()
year_2013 = raw_df.filter(raw_df['Violation Year'] == '2013').count()
year_2014 = raw_df.filter(raw_df['Violation Year'] == '2014').count()
year_2015 = raw_df.filter(raw_df['Violation Year'] == '2015').count()
Year = [1,2,3,4]
# Create list
Violations = [year_2012/1000, year_2013/1000, year_2014/1000, year_2015/1000]
LABELS = ["2012", "2013", "2014", "2015"]
# Plot
plt.bar(Year, Violations, align='center', color = 'green')
plt.title('Years vs Violations(k) bar-chart')
plt.xlabel('Years')
plt.ylabel("#Violations(k)")
plt.xticks(Year, LABELS)
plt.show()
