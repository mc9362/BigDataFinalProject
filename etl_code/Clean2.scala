var df = spark.read.csv("FirstCleanedData")
df.show()

// Renames columns
df = df.withColumnRenamed("_c0", "CAMIS").withColumnRenamed("_c1", "Borough").withColumnRenamed("_c2", "Score").withColumnRenamed("_c3", "Inspection Date").withColumnRenamed("_c4", "Violation Code").withColumnRenamed("_c5", "Critical Flag").withColumnRenamed("_c6", "Council District").withColumnRenamed("_c7", "Census Tract")
df.show()



//Code Cleaning
// I am doing Date formatting and Text formatting 


// Change the  date format from MM/DD/YYYY to YYYY-MM-DD
def changeFormat(date: Any):(String)={
    var d = date.toString()
    d = d.trim()
    var temp: String = d

    if (d.length() < 15){
        // This gets the year
        temp = d.slice(7,11)
        temp = temp + "-"
        // This gets the month 
        temp = temp + d.slice(1,3)
        temp = temp + "-"
        // This gets the day
        temp = temp + d.slice(4,6)
    }
    temp
}
// This applies the changeFormat function to the date column and adds a new column to the original dataframe with the updated date formats
var dates = df.select("Inspection Date")
var tempRdd = dates.rdd
var temp1 = tempRdd.map(changeFormat).toDF()
df = df.withColumn("New", monotonicallyIncreasingId)
temp1 = temp1.withColumn("New", monotonicallyIncreasingId)
var finalDf = df.join(temp1, df("New") === temp1("New"))



//Text formatting
// This makes makes everything upper case and trims any leading and trailing whitespace for the Borough and Critical Flag column
finalDf = finalDf.withColumn("Borough", trim(upper(col("Borough"))))
// I choose to NOT remove the spaces in the middle for the Critical Flag column, only the leading and trailing spaces 
finalDf = finalDf.withColumn("Critical Flag", trim(upper(col("Critical Flag"))))


//drop old/dummy columns, renaming columns
finalDf = finalDf.drop("Inspection Date").drop("New")
finalDf = finalDf.withColumnRenamed("value", "Inspection Date")
finalDf.show()

finalDf.write.option("header", "true").format("csv").save("cleanedDataFinal")


