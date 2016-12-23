# Here we plot a barchart of weekdays vs #violations
import pylab as plt
# Count #violations for each day
day_monday = raw_df.filter(raw_df['Violation Day of Week'] == 'MONDAY').count()
day_tuesday = raw_df.filter(raw_df['Violation Day of Week'] == 'TUESDAY').count()
day_wednesday = raw_df.filter(raw_df['Violation Day of Week'] == 'WEDNESDAY').count()
day_thursday = raw_df.filter(raw_df['Violation Day of Week'] == 'THURSDAY').count()
day_friday = raw_df.filter(raw_df['Violation Day of Week'] == 'FRIDAY').count()
day_saturday = raw_df.filter(raw_df['Violation Day of Week'] == 'SATURDAY').count()
day_sunday = raw_df.filter(raw_df['Violation Day of Week'] == 'SUNDAY').count()
total = raw_df.count()
DayOfWeekOfCall = [1,2,3,4,5,6,7]
# Create violation frequency per day list
Violations = [day_monday*100/total, day_tuesday*100/total, day_wednesday*100/total, day_thursday*100/total, day_friday*100/total, day_saturday*100/total, day_sunday*100/total]
# Plot barchart of days
LABELS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
plt.title('Day of Week vs Violations(%)')
plt.bar(DayOfWeekOfCall, Violations, align='center')
plt.xticks(DayOfWeekOfCall, LABELS)
plt.ylabel("%Violations")
plt.xlabel("Day of Week")
plt.show()
