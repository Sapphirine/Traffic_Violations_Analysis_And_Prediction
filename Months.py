import pylab as plt
# Calculate #violations for each month
Jan = raw_df.filter(raw_df['Violation Month'] == 1).count()
Feb = raw_df.filter(raw_df['Violation Month'] == 2).count()
Mar = raw_df.filter(raw_df['Violation Month'] == 3).count()
Apr = raw_df.filter(raw_df['Violation Month'] == 4).count()
May = raw_df.filter(raw_df['Violation Month'] == 5).count()
Jun = raw_df.filter(raw_df['Violation Month'] == 6).count()
Jul = raw_df.filter(raw_df['Violation Month'] == 7).count()
Aug = raw_df.filter(raw_df['Violation Month'] == 8).count()
Sep = raw_df.filter(raw_df['Violation Month'] == 9).count()
Oct = raw_df.filter(raw_df['Violation Month'] == 10).count()
Nov = raw_df.filter(raw_df['Violation Month'] == 11).count()
Dec = raw_df.filter(raw_df['Violation Month'] == 12).count()
Total = raw_df.count()
# Plot months vs violations percentage
Month = [1,2,3,4,5,6,7,8,9,10,11,12]
Violations = [Jan*100/Total, Feb*100/Total, Mar*100/Total, Apr*100/Total, May*100/Total, Jun*100/Total, Jul*100/Total, Aug*100/Total, Sep*100/Total, Oct*100/Total, Nov*100/Total, Dec*100/Total]
LABELS = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]
plt.bar(Month, Violations, align='center', color = 'yellow')
plt.title('Months vs %Violations bar-chart')
plt.xlabel('Months')
plt.ylabel("%Violations")
plt.xticks(Month, LABELS, rotation = '45')
plt.show()
