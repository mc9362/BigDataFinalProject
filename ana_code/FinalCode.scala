
import org.apache.spark.sql


var initialCleanedDf = spark.read.option("header", true).csv("cleanedDataFinal")

//renamed columns so there are no spaces in the column names
initialCleanedDf =initialCleanedDf.withColumnRenamed("Violation Code", "Violation_Code").withColumnRenamed("Critical Flag", "Critical_Flag").withColumnRenamed("Council District", "Council_District").withColumnRenamed("Inspection Date", "Inspection_Date").withColumnRenamed("Census Tract", "Census_Tract")
initialCleanedDf = initialCleanedDf.orderBy("CAMIS", "Inspection_Date", "Violation_Code")
initialCleanedDf.show()
initialCleanedDf.createTempView("data")


// combine all the flags for each inspection date for each restuarant (because different rows can be for the same inspection but with different violations and different flags)
// This helps get distinct inspection dates and it is necessary because if one inspection shows up multiple times, that will make the average score inaccurate
val CombinedFlags = spark.sql("SELECT first_value(CAMIS) as CAMIS,first_value(Inspection_Date) as Inspection_Date, first_value(Score) as Score, first_value(Borough) as Borough, first_value(Council_District) as Council_District, first_value(Census_Tract) as Census_Tract, collect_list(Critical_Flag) as Flags FROM data GROUP BY CAMIS, Inspection_Date ORDER BY CAMIS, Inspection_Date")
CombinedFlags.show()
CombinedFlags.createTempView("CombinedFlagsView")


// only look at the initial inspections for each restaurant
val withFirstInspectionFlag = spark.sql("SELECT *, first_value(Flags) OVER (PARTITION BY CAMIS ORDER BY Inspection_Date) as First_Inspection_Flag FROM CombinedFlagsView ORDER BY CAMIS, Inspection_Date")
withFirstInspectionFlag.show()
withFirstInspectionFlag.createTempView("withFirstInspectionFlagView")


// only choose data where the restaurant (identified by CAMIS numbers) has a initial inspection that has a critical flag
val firstViolation = spark.sql("SELECT * FROM withFirstInspectionFlagView WHERE array_contains(First_Inspection_Flag, 'CRITICAL') ")
firstViolation.show()
firstViolation.createTempView("firstViolationView")



// finds the score of the first inspection and average of all scores for each restaurant
val avgScoreTable = spark.sql("SELECT *, avg(Score) OVER (PARTITION BY CAMIS) as avg, first_value(Score) OVER (PARTITION BY CAMIS ORDER BY Inspection_Date) as firstScore FROM firstViolationView ORDER BY CAMIS, Inspection_Date")
avgScoreTable.show()
avgScoreTable.createTempView("avgScoreView")

// only need distinct CAMIS so the score change is only calculated once for each restuarant
val scoreChangeTable = spark.sql("SELECT DISTINCT CAMIS, Borough, Council_District, Census_Tract, avg, firstScore, avg - firstScore as diff FROM avgScoreView ORDER BY CAMIS")
scoreChangeTable.show()
scoreChangeTable.createTempView("scoreChangeView")




// *****NOTE: a lower score is better: the more points you get, the more violations you have 



// if avg is greater than initial, that means that the score is higher and did not improve
// NOTE that average of all scores works because currently, I am only concerned about increase/decrease in scores and not the magnitude 
// so if the average is greater than the initial score, the restaurant must have recieved subsequent scores that are higher (worse) than the inital score
val LowerScoreTable = spark.sql("SELECT * FROM scoreChangeView WHERE diff > 0")
val numLowerScore = LowerScoreTable.count().toDouble
println(numLowerScore)
// The output is 7386.0, meaning that 7386 restuarants that initially got a violation got a worse score overall in following inspections 
val totalRestaurants = scoreChangeTable.count().toDouble
println(totalRestaurants)
// The output is 22976.0, meaning that in there are 22976 restuarants that has a first inspection that recieved a critial violation

// Finds the overall proportion of restuarants that did NOT have score improvements
val proportionNoScoreImprovements = numLowerScore/totalRestaurants
println(proportionNoScoreImprovements)
// The output is 0.3214658774373259, which suggests that about 32 percent of restaurants that got an initial critical violation got worse scores in later inspections



// find the proportion of restuarants that did NOT have score improvements by borough, council district, census tract 



// by borough 

var numRestaurantsByBoro = scoreChangeTable.groupBy("Borough").count().orderBy("Borough")
numRestaurantsByBoro = numRestaurantsByBoro.withColumnRenamed("count", "totalByBoro")
numRestaurantsByBoro.show()

val numLowerScoreByBoro = LowerScoreTable.groupBy("Borough").count().orderBy("Borough")
numLowerScoreByBoro.show()

var combinedScoreByBoro = numRestaurantsByBoro.join(numLowerScoreByBoro,numRestaurantsByBoro("Borough")===numLowerScoreByBoro("Borough")).orderBy(numRestaurantsByBoro("Borough"))
combinedScoreByBoro = combinedScoreByBoro.withColumn("proportions", col("count")/col("totalByBoro"))
combinedScoreByBoro.show()



// by council district  

var numRestaurantsByCouncilDistrict = scoreChangeTable.groupBy("Council_District").count().orderBy("Council_District")
numRestaurantsByCouncilDistrict = numRestaurantsByCouncilDistrict.withColumnRenamed("count", "totalByCouncilDistrict")
numRestaurantsByCouncilDistrict.show()

val numLowerScoreByCouncilDistrict = LowerScoreTable.groupBy("Council_District").count().orderBy("Council_District")
numLowerScoreByCouncilDistrict.show()

var combinedScoreByCouncilDistrict = numRestaurantsByCouncilDistrict.join(numLowerScoreByCouncilDistrict,numRestaurantsByCouncilDistrict("Council_District")===numLowerScoreByCouncilDistrict("Council_District") ).orderBy(numRestaurantsByCouncilDistrict("Council_District"))
combinedScoreByCouncilDistrict = combinedScoreByCouncilDistrict.withColumn("proportions", col("count")/col("totalByCouncilDistrict"))
combinedScoreByCouncilDistrict.show()

// examining the lower proportions 
combinedScoreByCouncilDistrict.orderBy(asc("proportions")).show()
// examining the higher proportions 
combinedScoreByCouncilDistrict.orderBy(desc("proportions")).show()



// by census tract

var numRestaurantsByCensusTract = scoreChangeTable.groupBy("Census_Tract").count().orderBy("Census_Tract")
numRestaurantsByCensusTract = numRestaurantsByCensusTract.withColumnRenamed("count", "totalByCensusTract")
numRestaurantsByCensusTract.show()

val numLowerScoreByCensusTract = LowerScoreTable.groupBy("Census_Tract").count().orderBy("Census_Tract")
numLowerScoreByCensusTract.show()

var combinedScoreByCensusTract = numRestaurantsByCensusTract.join(numLowerScoreByCensusTract,numRestaurantsByCensusTract("Census_Tract")===numLowerScoreByCensusTract("Census_Tract")).orderBy(numRestaurantsByCensusTract("Census_Tract"))
combinedScoreByCensusTract = combinedScoreByCensusTract.withColumn("proportions", col("count")/col("totalByCensusTract"))
combinedScoreByCensusTract.show()

// examining the lower proportions 
combinedScoreByCensusTract.orderBy(asc("proportions")).show()

// examining the higher proportions 
// By examining the data, just looking at the higher proportions might not be very useful  
// This is because there might a very small number of restuarants that had initial violations to begin with for some Census_Tracts (like 1 or 2) 
combinedScoreByCensusTract.orderBy(desc("proportions")).show()










// checks if there are repeating violations

// First filter the initial dataframe so there are only  violations that are critical (only critical violations are relevant for this part)
val criticalFlagsOnly = spark.sql("SELECT * FROM data WHERE Critical_Flag = 'CRITICAL' ORDER BY CAMIS, Inspection_Date, Violation_Code")
criticalFlagsOnly.show()
criticalFlagsOnly.createTempView("criticalFlagsOnlyView")

// Combine all the critical violations for each inspection 
val CombinedViolations = spark.sql("SELECT first_value(CAMIS) as CAMIS,first_value(Inspection_Date) as Inspection_Date, first_value(Borough) as Borough, first_value(Council_District) as Council_District, first_value(Census_Tract) as Census_Tract, collect_list(Violation_Code) as Violations FROM criticalFlagsOnlyView GROUP BY CAMIS, Inspection_Date ORDER BY CAMIS, Inspection_Date")
CombinedViolations.show()
CombinedViolations.createTempView("CombinedViolationsView")


// only look at the initial inspections for each restaurant
val firstInspectionViolations = spark.sql("SELECT CAMIS, Inspection_Date, first_value(Violations) OVER (PARTITION BY CAMIS ORDER BY Inspection_Date) as First_Inspection_Violations FROM CombinedViolationsView ORDER BY CAMIS, Inspection_Date")
firstInspectionViolations.show()
firstInspectionViolations.createTempView("firstInspectionViolationsView")


// combine the firstInspectionViolations table with the inital data table 
// this matches each row with the corresponding first Inspection Violations Array
val tempViolationsDf = spark.sql("SELECT data.*,firstInspectionViolationsView.First_Inspection_Violations FROM data LEFT JOIN firstInspectionViolationsView ON data.CAMIS = firstInspectionViolationsView.CAMIS AND data.Inspection_Date = firstInspectionViolationsView.Inspection_Date ORDER BY data.CAMIS, data.Inspection_Date, data.Violation_Code")
tempViolationsDf.show()
tempViolationsDf.createTempView("tempViolationsView")


// combine firstViolationView(data with restuarants that has a critical flag in its initial inspection) with tempViolationsView  
val tempViolationsDf2 = spark.sql("SELECT firstViolationView.*, tempViolationsView.Violation_Code, tempViolationsView.First_Inspection_Violations FROM firstViolationView LEFT JOIN tempViolationsView ON firstViolationView.CAMIS = tempViolationsView.CAMIS AND firstViolationView.Inspection_Date = tempViolationsView.Inspection_Date ORDER BY firstViolationView.CAMIS, firstViolationView.Inspection_Date, tempViolationsView.Violation_Code")
tempViolationsDf2.show()
tempViolationsDf2.createTempView("tempViolationsView2")


// filter the previous table so that only repeated (inital) violations are shown 
// also adds a column for the number of critical violations in the first inspection 
val repeatedViolations = spark.sql("SELECT CAMIS, Inspection_Date, Borough, Council_District, Census_Tract, Violation_Code, First_Inspection_Violations, size(First_Inspection_Violations) as numInitialViolation FROM tempViolationsView2 WHERE array_contains(First_Inspection_Violations,Violation_Code) ORDER BY CAMIS, Inspection_Date, Violation_Code")
repeatedViolations.show()
repeatedViolations.createTempView("repeatedViolations")


// this adds up the violations that matches the intial violations (repeated violations) for each restaurant
val repeatedCounts = spark.sql("SELECT * , count(*) OVER (PARTITION BY CAMIS) as totalNumViolations FROM repeatedViolations ORDER BY CAMIS, Inspection_Date, Violation_Code")
repeatedCounts.show()
repeatedCounts.createTempView("repeatedCounts")

// subtract the numInitialViolation from totalNumViolations (to remove the overcount)
// because the initial violations were included, need to subtract them to find the actual count of repeated violations 
// Also find DISTINCT because the actualRepeatedViolations will be the same for each restaurant 
val actualRepeatedViolations = spark.sql("SELECT DISTINCT CAMIS, Borough, Council_District, Census_Tract, totalNumViolations - numInitialViolation as actualRepeatedViolations FROM repeatedCounts ORDER BY CAMIS")
actualRepeatedViolations.show()


// Filter the previous table to drop the rows that have 0 repeat violations 
val repeatedViolationRestaurantDf = actualRepeatedViolations.filter(actualRepeatedViolations("actualRepeatedViolations") > 0) 
repeatedViolationRestaurantDf.show()


// This finds the number of restaurants that had repeat violations
// It should return 10168.0
val repeatedViolationRestaurantCount = repeatedViolationRestaurantDf.count().toDouble
println(repeatedViolationRestaurantCount)
// The total number of restaurants that had initial violations is 22976.0
// This was found previously
println(totalRestaurants)
// Finds the overall proportion of restuarants that had repeat violations 
val proportionRepeatViolation = repeatedViolationRestaurantCount/totalRestaurants
println(proportionRepeatViolation)
// The output is 0.44254874651810583, which means that about 44% of restaurants that got an initial critical violation got those same violations in later inspections 




// find the proportion of restuarants that had repeated violations by borough, council district, census tract 
// the total number of restuarants that had inital violations by borough, council district, census tract has been calculated previously 
// it is the dataframes: numRestaurantsByBoro, numRestaurantsByCouncilDistrict, numRestaurantsByCensusTract



// by borough 

val numRepeatedViolationRestaurantByBoro = repeatedViolationRestaurantDf.groupBy("Borough").count().orderBy("Borough")
numRepeatedViolationRestaurantByBoro.show()

var combinedRepeatedViolationByBoro = numRestaurantsByBoro.join(numRepeatedViolationRestaurantByBoro,numRestaurantsByBoro("Borough")===numRepeatedViolationRestaurantByBoro("Borough")).orderBy(numRestaurantsByBoro("Borough"))
combinedRepeatedViolationByBoro = combinedRepeatedViolationByBoro.withColumn("proportions", col("count")/col("totalByBoro"))
combinedRepeatedViolationByBoro.show()



// by council distict

val numRepeatedViolationByCouncilDistrict = repeatedViolationRestaurantDf.groupBy("Council_District").count().orderBy("Council_District")
numRepeatedViolationByCouncilDistrict.show()

var combinedRepeatedViolationByCouncilDistrict = numRestaurantsByCouncilDistrict.join(numRepeatedViolationByCouncilDistrict,numRestaurantsByCouncilDistrict("Council_District")===numRepeatedViolationByCouncilDistrict("Council_District") ).orderBy(numRestaurantsByCouncilDistrict("Council_District"))
combinedRepeatedViolationByCouncilDistrict = combinedRepeatedViolationByCouncilDistrict.withColumn("proportions", col("count")/col("totalByCouncilDistrict"))
combinedRepeatedViolationByCouncilDistrict.show()

// examining the lower proportions 
combinedRepeatedViolationByCouncilDistrict.orderBy(asc("proportions")).show()
// examining the higher proportions 
combinedRepeatedViolationByCouncilDistrict.orderBy(desc("proportions")).show()




// by census tract

val numRepeatedViolationByCensusTract = repeatedViolationRestaurantDf.groupBy("Census_Tract").count().orderBy("Census_Tract")
numRepeatedViolationByCensusTract.show()

var combinedRepeatedViolationByCensusTract = numRestaurantsByCensusTract.join(numRepeatedViolationByCensusTract,numRestaurantsByCensusTract("Census_Tract")===numRepeatedViolationByCensusTract("Census_Tract")).orderBy(numRestaurantsByCensusTract("Census_Tract"))
combinedRepeatedViolationByCensusTract = combinedRepeatedViolationByCensusTract.withColumn("proportions", col("count")/col("totalByCensusTract"))
combinedRepeatedViolationByCensusTract.show()

// examining the lower proportions 
combinedRepeatedViolationByCensusTract.orderBy(asc("proportions")).show()
// examining the higher proportions 
combinedRepeatedViolationByCensusTract.orderBy(desc("proportions")).show()
// By examining the data, the some of the lower and higher proportions data might not be very useful  
// This is because there some census tracts with a small number of restuarants that had initial violations so it is more likely for those areas to have extreme proportions  
















