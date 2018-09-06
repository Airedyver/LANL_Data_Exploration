### LANL_Data_Exploration ###
An exercise in Scala, Spark, and Python to work through a 71 MB file

LANL has open source data with a small explanation here : https://csr.lanl.gov/data/cyber1/

## Introduction ##
Because the data set represents over 1 billion points of authentication logs, I did not have the capacity to analyze a complete data set. Because it's time-based I decided to cut the dataset by every 40 lines to get an accurate contextual sample. Code for this will be in the 'Splitting_Auth_txt.ipynb' 
Next, I reviewed the data in a Scala IDE called Eclipse, if you want to run the Scala IDE you will need a Java Development Kit. , Frank Kane has a great instructional video that shows you how to download Spark and Scala IDE https://www.youtube.com/watch?v=WlE7RNdtfwE&t=333s. I explored both red team and authorization data, within 'Next_Rev_Scala_Data_Exploration.scala', you can find this within the 'Data_Exploration' folder.

I originally wanted to join the red team and auth_sample dataframes together but was not able to because red teams information was not unique enough to make into one dataframe. I wanted to use this new data frame and place it into a Machine Learning Algorithm. My attempts can be found in the folder "Failures". Yep, I have a failures file.  

After re-reading the instructions, I noticed that I could make a binary classification system through Databricks, almost right out of their instructional notebook, with a little bit of tweeking of course. 

The Auth_Table_Upload.ipynb file is meant for a large data sample, it is not tested, but came from the API section of the databricks documentation. USE AT YOUR OWN RISK, AWS and Databricks may charge you for uploading such a large data set. I'd suggest just sticking with my 'Next Rev Binary Classifier'.ipynb file. In it, there are 3 different types of binary classifiers that were used, all are supervised learning methods. First is Logistic Regression, Second is Decision Tree, third is Random Forest using 10 trees. Using all of these models will show the eventual best model for the data I have. I did also consider a KMeans clustering algorithm, which is an unsupervised learning algorithm, but I preferrred the more hands on approach for the data I have.

##Instructions## 

Because the data set is so large, please download auth.txt from https://csr.lanl.gov/data/cyber1/, then use my 'Splitting_Auth_txt.ipynb' jupyter notebook to create your own data sample, called 'newsample.csv' for Data Exploration, and smallersample.csv for Machine Learning.  A good reference to get your SCALA IDE up and running is through Frank Kane's youtube video. Afterwards, might I recommend looking at the machine learning notebook I did called 'Next Rev Binary Classifier'? this is a databricks jupyter notebook. More exploration can be found in Data_Exploration, I have a graph of the amount of Failures vs. Successes in Next_Rev_Bar_Graph notebook, this is a databricks jupyter notebook as well. Then explore Failures to find my findings on joining the redteam and auth.txt tables together. Databricks has their own types of jupyter notebook and its easy to create a free community addition platform.They have a lot of pre-baked spark goodies and make spark fairly easy to learn. 

## Explanation of the Data ##
The data is authorization logs from a windows based system. It is public data from Los Alamos National Labs. Source User Domain and Destination User Domain are the domain names of the Source Computer and Destination Computer. Authentication type; according to itprotoday.com in "comparing-windows-kerberos-and-ntlm-authentication-protocols", "NTLM is a challenge/response-based authentication protocol that is the default authentication protocol of Windows NT 4.0 and earlier Windows versions" Kerberos at this point in time is the default authentication protocol for Windows 200 and above.Negotiate is Kerberos under Internet Explorer(source: https://www.itprotoday.com/security/comparing-windows-kerberos-and-ntlm-authentication-protocols.) log on type is Network, Service,Batch, ? , Interactive, Network Cleartext, and New Credentials, Authentication Orientation talks about Logging On or Logging Off. SuccessorFail is dealing with if the Authorization suceeded or failed.

  

## Reasonings ##
#I did not get rid of rows with '?' for a reason. Because I noticed that most of the Failures came from '?' data. '?'s should not be deleted. Sometimes it was a ? on the Authentication Type to Log Off a system because you do not need an authentication type to Log Off a system. 

## More to Explore ## 

I would really like to see a graphically described decision tree, but ran out of time. I would also like to explore more of K-Means CLuster Machine learning in databricks, but I think I may have exceeded my data limit for them. K-Means likely would have been a better fit, because most of the research I have read has talked about detecting anamolies through K-Means. 

If you're interested in what I have learned so far :
a primer for anomaly detection
https://www.datascience.com/blog/python-anomaly-detection
https://mapr.com/ebooks/spark/08-unsupervised-anomaly-detection-apache-spark.html
https://www.allerin.com/blog/machine-learning-for-anomaly-detection

How to tailor a large dataset for Machine Learning purposes
https://machinelearningmastery.com/large-data-files-machine-learning/

a good example of data exploration using databricks
https://databricks.com/blog/2018/08/09/loan-risk-analysis-with-xgboost-and-databricks-runtime-for-machine-learning.html




