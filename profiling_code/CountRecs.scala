// This creates the dataframe from the csv
val df = spark.read.option("header", "true").csv("DOHMH_New_York_City_Restaurant_Inspection_Results_20240401.csv")

// This counts the number of records and prints them
df.count()
df.show()

// This map the records to a key and a value and count the number of records
// This basically checks if there are duplicates 
val distinctCols = df.map{row=> (row(0)+" " + row(1) + " " + row(2) + " " + row(3) + " " + row(4) + " " + row(5) + " " + row(6) + " " + row(7) + " " + row(8)+ " " + row(9)+ " " + row(10)+ " " + row(11)+ " " + row(12)+ " " + row(13)+ " " + row(14)+ " " + row(15)+ " " + row(16)+ " " + row(17)+ " " + row(18)+ " " + row(19)+ " " + row(20)+ " " + row(21)+ " " + row(22)+ " " + row(23)+ " " + row(24)+ " " + row(25)+ " " + row(26), 1)}
distinctCols.groupBy("_1").sum("_2").orderBy(desc("sum(_2)")).show()


// This selects the columns needed
val ColsNeeded = df.select("CAMIS", "BORO", "SCORE","INSPECTION DATE", "VIOLATION CODE", "CRITICAL FLAG", "Council District", "Census Tract")

// This finds the distict values in each column and the number of times that the values appear
val CAMISCol = ColsNeeded.groupBy("CAMIS").count().orderBy(desc("count"))
CAMISCol.show()
val BoroCol = ColsNeeded.groupBy("BORO").count().orderBy(desc("count"))
BoroCol.show()
val ScoreCol = ColsNeeded.groupBy("SCORE").count().orderBy(desc("count"))
ScoreCol.show()
val InspectionDateCol = ColsNeeded.groupBy("INSPECTION DATE").count().orderBy(desc("count"))
InspectionDateCol.show()
val ViolationCodeCol = ColsNeeded.groupBy("VIOLATION CODE").count().orderBy(desc("count"))
ViolationCodeCol.show()
val CriticalFlagCol = ColsNeeded.groupBy("CRITICAL FLAG").count().orderBy(desc("count"))
CriticalFlagCol.show()
val CouncilDistrictCol = ColsNeeded.groupBy("Council District").count().orderBy(desc("count"))
CouncilDistrictCol.show()
val CensusTractCol = ColsNeeded.groupBy("Census Tract").count().orderBy(desc("count"))
CensusTractCol.show()

//Additional Code for exploring data
//Exploring null values in the SCORE Column and corresponding values in CRITICAL FLAG column
val exploreDf = df.select("SCORE", "CRITICAL FLAG")
exploreDf.filter(row=> row(0)==null).show()

val typeFlagForNullsDf = exploreDf.filter(row=> row(0)==null).groupBy("CRITICAL FLAG").count()
typeFlagForNullsDf.show()
//Most of the scores with null values have non critical or no violations (as indicated by the Not Critical and Not Applicable flags)


