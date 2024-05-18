NOTE: The order that the code should be run is:
1. /data_ingest: dataIngestion.sh
2. /profiling_code : CountRecs.scala
3. /etl_code: Clean.scala
4. /ana_code: FirstCode.scala
5. /etl_code: Clean2.scala
6. /ana_code: FinalCode.scala

Specific directions for each step is written below.

The screenshot results of each step are available in the screenshots directory.
Note that when running the code, for some dataframes, the row order may not be exactly the same as what is displayed in the screenshots because the rows may not be sorted or if the value used to sort the rows is the same for some rows.


Access to the following directories/files has been provided in HDFS(more details in the steps below):
1. DOHMH_New_York_City_Restaurant_Inspection_Results_20240401.csv
2. FirstCleanedData
3. cleanedDataFinal


Step 1: /data_ingest: dataIngestion.sh

I used the dataset DOHMH New York City Restaurant Inspection Results from NYC Open Data using the link: https://data.cityofnewyork.us/Health/DOHMH-New-York-City-Restaurant-Inspection- Results/43nn-pn8j/about_data . I exported the dataset as a csv file (which downloads it into my local machine/computer) and then I uploaded it into Dataproc by clicking the "Upload File" button on the upper right corner of the Dataproc website and choosing the csv file. Then, I uploaded that from Dataproc into HDFS by using the "hdfs dfs -put DOHMH_New_York_City_Restaurant_Inspection_Results_20240401.csv" command(this is the command that is placed in the dataIngestion.sh file. You can also upload the data by uploading the "dataIngestion.sh" file into Dataproc and running the "sh dataIngestion.sh" command in the Dataproc terminal to run the -put command). 

***NOTE*** There is a screenshot of using the "hdfs dfs -put DOHMH_New_York_City_Restaurant_Inspection_Results_20240401.csv" command to enter the data into HDFS but it shows a error (File exists). This data was uploaded already because this data was the same data used for previous homeworks related to this project and I did not want to delete it and re-upload it (just in case if it effects previous homework assignments since they have not been graded yet) but just note that if I was entering the data for the first time, it would not show an error and it will be uploaded into HDFS. 

***NOTE***: Please use the input data in HDFS instead of using the link given and uploading the data to HDFS because the data on the NYC Open Data website gets updated every day so the current data would not be the same as the input dataset. The initial input dataset should be named "DOHMH_New_York_City_Restaurant_Inspection_Results_20240401.csv" in HDFS and access to the dataset has been provided.



Step 2: /profiling_code : CountRecs.scala

In the /profiling_code directory, there is a file named "CountRecs.scala". It uses the file "DOHMH_New_York_City_Restaurant_Inspection_Results_20240401.csv" as input data.
This code is for basic data profiling and getting to know the data. 
To run the code, you will first need to upload the "CountRecs.scala" file into Dataproc by clicking the "Upload File" button on the upper right corner of the Dataproc website and choosing to upload the "CountRecs.scala" file. 
Then, enter the "hdfs dfs -put CountRecs.scala" command into Dataproc to put the CountRecs.scala file into HDFS. 
Then, to run the code, you can use the command "spark-shell --deploy-mode client -i CountRecs.scala" to start the Spark shell and run all the commands in the file.
Only the results of running the commands will be shown (the commands are not shown) and the results of the commands are NOT saved to HDFS. 
Note that for some of the resulting dataframes, the order of some of the rows may be different if you run it again because it is sorted by count and sometimes rows may have the same counts.



Step 3: /etl_code: Clean.scala

In the /etl_code directory, there is a file named "Clean.scala". It uses the file "DOHMH_New_York_City_Restaurant_Inspection_Results_20240401.csv" as input data. 
This code is for the initial basic cleaning of data(a second cleaning will be done in Step 5).
To run the code, you will first need to upload the "Clean.scala" file into Dataproc by clicking the "Upload File" button on the upper right corner of the Dataproc website and choosing to upload the "Clean.scala" file. 
Then, enter the "hdfs dfs -put Clean.scala" command into Dataproc to put the Clean.scala file into HDFS. 
Then, to run the code, you can use the command "spark-shell --deploy-mode client -i Clean.scala" to start the Spark shell and run all the commands in the file.
Only the results of running the commands will be shown (the commands are not shown) and the final cleaned dataset is then written to HDFS with the file directory name "FirstCleanedData" and access to this cleaned dataset has been provided.
To replicate this step, please make sure that there is no directory named "FirstCleanedData" in your HDFS before running the file. 
Note that for some of the resulting dataframes, the order of some of the rows may be different than the screenshots because the data is not sorted.



Step 4: /ana_code: FirstCode.scala

In the /ana_code directory, there is a file named "FirstCode.scala". It uses the csv file directory "FirstCleanedData" as input data.
This code is for basic analysis/statistics of the data in the "Score" column.
To run the code, you will first need to upload the "FirstCode.scala" file into Dataproc by clicking the "Upload File" button on the upper right corner of the Dataproc website and choosing to upload the "FirstCode.scala" file. 
Then, enter the "hdfs dfs -put FirstCode.scala" command into Dataproc to put the FirstCode.scala file into HDFS. 
Then, to run the code, you can use the command "spark-shell --deploy-mode client -i FirstCode.scala" to start the Spark shell and run all the commands in the file.
Only the results of running the commands will be shown (the commands are not shown) and the results of the commands are NOT saved to HDFS.



Step 5: /etl_code: Clean2.scala

In the /etl_code directory, there is a file named "Clean2.scala". It uses the csv file directory "FirstCleanedData" as input data. 
This code is for the second cleaning of the dataset.
To run the code, you will first need to upload the "Clean2.scala" file into Dataproc by clicking the "Upload File" button on the upper right corner of the Dataproc website and choosing to upload the "Clean2.scala" file. 
Then, enter the "hdfs dfs -put Clean2.scala" command into Dataproc to put the Clean2.scala file into HDFS. 
Then, to run the code, you can use the command "spark-shell --deploy-mode client -i Clean2.scala" to start the Spark shell and run all the commands in the file.
Only the results of running the commands will be shown (the commands are not shown) and the final cleaned dataset is then written to HDFS with the file directory name "cleanedDataFinal" and access to this cleaned dataset has been provided.
To replicate this step, please make sure that there is no directory named "cleanedDataFinal" in your HDFS before running the file. 




Step 6: /ana_code: FinalCode.scala

In the /ana_code directory, there is a file named "FinalCode.scala". It uses the csv file directory "cleanedDataFinal" as input data.
This code is for the main analysis of the data.
To run the code, you will first need to upload the "FinalCode.scala" file into Dataproc by clicking the "Upload File" button on the upper right corner of the Dataproc website and choosing to upload the "FinalCode.scala" file. 
Then, enter the "hdfs dfs -put FinalCode.scala" command into Dataproc to put the FinalCode.scala file into HDFS. 
Then, to run the code, you can use the command "spark-shell --deploy-mode client -i FinalCode.scala" to start the Spark shell and run all the commands in the file.
Only the results of running the commands will be shown (the commands are not shown) and the results of the commands are NOT saved to HDFS.
Note that for some of the resulting dataframes, the order of some of the rows may be different than the screenshots. For example, in dataframes like "combinedScoreByCensusTract", some rows may have the same values in the proportions column so when the dataframes are displayed using sort ascending and descending, the row order may be a little different.

























