# Traffic_Violations_Analysis_And_Prediction
Big Data Analytics Final Project For Georgios Charitos - gc2662 , Aditya Bagri - aab2234 and Srinidhi Srinivasan - ss4974

Link to dataset : https://data.ny.gov/Transportation/Traffic-Tickets-Issued-Four-Year-Window/q4hy-kbtf/data

All files must be executed in pyspark with the

>>>execfile("path_to_file")

The first python file to be executed is the CreateDataframe.py. It creates a dataframe of the dataset.

The files below have to do with the first part of the analysis and must be executed after the execution of the CreateDataframe.py because they use the dataset dataframe. We have run numerous queries below and they are named in a way that suggests what the query is: 

 - Age.py
 - Age23Top5Violations.py
 - Age21_23.py
 - GenderPieChart.py
 - PhoneViolations.py
 - State.py
 - Years.py
 - Month.py
 - Top10Violations.py

We have done the analysis in two part, and second one is included in the python file below using the Seaborn Library:

 - seabornTwo.py : This file contains the python code for the visualizations using the Seaborn Library. Comments include instructions for uploading the data as a pandas data frame and using the various functions with a combination of arguments to execute the numerous plots availavble in the library.

The files below have to do with the prediction:

 - PredictGender.py
 - PredictAge.py
 - PredictAge21_25.py
 - Random_forest_regression_age.py - Random Forest Regression with 2 trees to predict the Age Field
 - Random_forest_regression_years.py - Random Forest Regression with 40 trees to predict the Years Field
 - Svm_with_sgd_gender.py - Linear SVM with SGD optimization to predict the gender
 - K_means_2_clusters.py - K-means Clustering to create 2 clusters of age
 - K_means_15_clusters.py - K-means Clustering to create 15 clusters of age
 - Linear_regression_with_sgd.py - Linear Regression with SGD optimization
 - Logistic_regression_with_lbfgs_age.py - Logistic Regression with L-BFGS optimization to predict age
 - Naive_bayes.py - Naive Bayes Classification for the given database
 - Temp.txt - Data set including the fields with numerical values of the dataset
 - TryFour.py - Create a dataframe with required fields from the complete dataset
 - Convert.py - Converts the dataframe created from TryFour.py to make a appropriate CSV format
 - Replace.py - Replacing the undefined values in the dataset to create complete dataset in txt format
 - Try.py - Converting the numerical values in text file from convert.py to create the txt file with field values in float data type
 - Try2.py - Replacing the undefined values in the dataset to create complete dataset in CSV format
 
 
*Remember to change the path of the dataset
