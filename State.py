
# Plot a pie chart for New York and rest of the states and a barchart of #violation for each state except for NY
# Import plotting library
import matplotlib.pyplot as plt2
# Create dataframe for the states
states_df = raw_df.rollup('State of License').count().sort('count',ascending=False)
# Create list for the states
states = states_df.select('count').rdd.flatMap(lambda y: y).collect()
total = states[0]
ny = states[1]
unknown = states[2]
rest = total-ny-unknown
# Plot a pie chart for new york, unknown and rest of the states
labels = 'NEW YORK', 'OTHER STATES', 'UNKNOWN'
sizes = [ny, unknown, rest]
colors = ['orange', 'blue', 'white']
explode = (0, 0.1, 0)  # explode 1st slice
plt2.pie(sizes, explode=explode, labels=labels, colors=colors, autopct='%1.1f%%', shadow=True, startangle=140)
plt2.axis('equal')
plt2.show()
# Plot barchart for top16 states
State = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16]
Violations = [states[3]*100/rest, states[4]*100/rest, states[5]*100/rest, states[6]*100/rest, states[7]*100/rest, states[8]*100/rest, states[9]*100/rest, states[10]*100/rest, states[11]*100/rest, states[12]*100/rest, states[13]*100/rest, states[14]*100/rest, states[15]*100/rest, states[16]*100/rest, states[17]*100/rest, states[18]*100/rest,]
LABELS = ["NJ", "PA", "CT", "FL", "MA", "OC", "MD", "VA", "CA", "NC", "WA", "QC", "OH", "GA", "VT", "TX"]
plt.title('Violations Percentage vs State License Plates')
plt.bar(State, Violations, align='center', color='red')
plt.xlabel('State')
plt.ylabel("Violations Percentage")
plt.xticks(State, LABELS)
plt.show()
