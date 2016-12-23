import pandas as pd
import seaborn as sns

sns.set_style("whitegrid")

DataFile = '/Users/Aditi/Desktop/Spark/spark-2.0.1-bin-hadoop2.7/BigDataProject/Traffic_Tickets_Issued__Four_Year_Window.csv'

df = pd.read_csv(DataFile) #loading the data set as a pandas data frame

sns.lmplot('Violation Year', 'Violation Month', data=df, fit_reg=False) #lmplot visualization

sns.swarmplot(x="Violation Year", y="Age at Violation", hue = "Gender", data=df) #swarmplot broken out by Gender

sns.boxplot(x="Violation Day of Week", y="Age at Violation", hue="Gender", data=df, whis=np.inf) #boxplot broken out by Gender

sns.violinplot(x="Violation Day of Week", y="Age at Violation", data=df, inner=None) #Violinplot visualization

sns.swarmplot(x="Violation Day of Week", y="Age at Violation", data=df, color="white", edgecolor="gray")

sns.violinplot(x="Violation Day of Week", y="Age at Violation", hue="Gender", data=df, inner=None) #combination of Violinplot and Swarmplot

sns.plt.show()

