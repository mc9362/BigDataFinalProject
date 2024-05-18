val df = spark.read.option("header", "true").csv("DOHMH_New_York_City_Restaurant_Inspection_Results_20240401.csv")
df.show()

// This selects only the columns needed (drops columns that are not needed)
val FinalCols = df.select("CAMIS", "BORO", "SCORE","INSPECTION DATE", "VIOLATION CODE", "CRITICAL FLAG", "Council District", "Census Tract")
FinalCols.show()

// This removes the rows with inspections that have not been conducted (given inspection date "01/01/1900")
val temp = FinalCols.filter(row => row(3)!="01/01/1900")
temp.show()

// This removes the rows with missing data for the Census tract and council districts
val temp2 = temp.filter(row=> row(6)!=null && row(7)!=null)
temp2.show()

//Remove duplicate records 
val temp3 = temp2.distinct()
temp3.show()

//Replace the nulls in the Violation Code Column with N/A
//Replace the nulls in the Score column with 0
val replaceNull = Map("VIOLATION CODE" -> "N/A", "SCORE" -> "0")
val temp4 = temp3.na.fill(replaceNull)
temp4.show()

temp4.write.format("csv").save("FirstCleanedData")

