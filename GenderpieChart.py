# Import plotting library
import matplotlib.pyplot as plt2
# Calculate gender violations
males = raw_df.filter(raw_df['Gender'] == 'M').count()
females = raw_df.filter(raw_df['Gender'] == 'F').count()
unknown = raw_df.count() - (males + females)
# Plot the chart
labels = 'Male', 'Female', 'Unknown'
sizes = [males, females, unknown]
colors = ['blue', 'pink', 'yellow']
explode = (0.1, 0, 0)  # explode 1st slice
plt2.pie(sizes, explode=explode, labels=labels, colors=colors, autopct='%1.1f%%', shadow=True, startangle=140)
plt2.axis('equal')
plt2.show()
