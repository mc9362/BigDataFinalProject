// This creates the dataframe from the directory of csv files of the cleaned data
var df = spark.read.csv("FirstCleanedData")
df.show()

// Renames columns
df = df.withColumnRenamed("_c0", "CAMIS").withColumnRenamed("_c1", "Borough").withColumnRenamed("_c2", "Score").withColumnRenamed("_c3", "Inspection Date").withColumnRenamed("_c4", "Violation Code").withColumnRenamed("_c5", "Critical Flag").withColumnRenamed("_c6", "Council District").withColumnRenamed("_c7", "Census Tract")
df.show()


// The only numerical data is the Score column
// Even though the Council District and Census Tract columns have numbers, they are categorical data
df = df.withColumn("Score", col("Score").cast("int"))
var scoreCol = df.select("Score")
scoreCol.show()


// Mean of data 
// Mean is 22.315114256527302 
var meanScoreDf = scoreCol.agg(mean("Score"))
var meanScoreTypeAny = meanScoreDf.rdd.collect()(0)(0) // to extract the value from the dataframe
var meanScoreValDouble = meanScoreTypeAny.toString.toDouble // convert the type from Any to Double
println(meanScoreValDouble)


// Median of Score column
// Median is 18
var numRecords = scoreCol.count().toInt
var medianIndex = (numRecords/2) -1 //subtract 1 because indexing starts from 0
scoreCol = scoreCol.sort("Score")
var scoreRdd = scoreCol.rdd
println(scoreRdd.collect()(medianIndex)) 

// Mode of data
scoreCol.groupBy("Score").count().orderBy(desc("count")).show()
// Since the score of 12 has the highest count, it means that it appears the most frequently and is the mode
// Mode is 12


//Standard Deviation of Score column 
//Finds standard deviation using standard devation formula
// The standard deviation is 19.824466418385057

import scala.math.pow
def squareDiff (score:Any):(Double) = {
    var x = score.toString.slice(1,2).toInt // The values stored as '[x]', slice to extract x
    var temp = Math.pow((x-meanScoreValDouble),2)
    temp
}
val differences = scoreRdd.map(squareDiff)
val sumOfDiffSquared = differences.sum
val ScoreStddev = Math.pow(sumOfDiffSquared/numRecords, 0.5)
print(ScoreStddev)




