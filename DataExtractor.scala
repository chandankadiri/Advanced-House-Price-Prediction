import org.apache.spark
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class DataExtractor(dataDirectory: String, sparkSession: SparkSession) {
  val rawTrainingData = sparkSession.read.format("com.databricks.spark.csv").option("delimiter", ",").option("header", "true").option("nullValue", "NA").schema(trainingSchema).load(dataDirectory + "train.csv")
  val rawTestData = sparkSession.read.format("com.databricks.spark.csv").option("delimiter", ",").option("header", "true").option("nullValue", "NA").schema(testSchema).load(dataDirectory + "test.csv")

  def trainingData: sql.DataFrame = {
    val countFeaturesTrainingDF = rawTrainingData.select("Id", "SalePrice", "MSSubClass", "LotArea", "OverallQual", "OverallCond", "YearBuilt", "YearRemodAdd", "BsmtFinSF1", "BsmtFinSF2", "BsmtUnfSF", "TotalBsmtSF", "1stFlrSF", "2ndFlrSF", "LowQualFinSF", "GrLivArea", "BsmtFullBath", "BsmtHalfBath", "FullBath", "HalfBath", "BedroomAbvGr", "KitchenAbvGr", "TotRmsAbvGrd", "Fireplaces", "GarageCars", "GarageArea", "WoodDeckSF", "OpenPorchSF", "EnclosedPorch", "3SsnPorch", "ScreenPorch", "PoolArea", "MoSold", "YrSold")
    countFeaturesTrainingDF.na.fill(0)
  }

  def testData: sql.DataFrame = {
    val countFeaturesTestDF = rawTestData.select("Id", "MSSubClass", "LotArea", "OverallQual", "OverallCond", "YearBuilt", "YearRemodAdd", "BsmtFinSF1", "BsmtFinSF2", "BsmtUnfSF", "TotalBsmtSF", "1stFlrSF", "2ndFlrSF", "LowQualFinSF", "GrLivArea", "BsmtFullBath", "BsmtHalfBath", "FullBath", "HalfBath", "BedroomAbvGr", "KitchenAbvGr", "TotRmsAbvGrd", "Fireplaces", "GarageCars", "GarageArea", "WoodDeckSF", "OpenPorchSF", "EnclosedPorch", "3SsnPorch", "ScreenPorch", "PoolArea", "MoSold", "YrSold")
    countFeaturesTestDF.na.fill(0)
  }

  private
  def commonSchema = Array(
    StructField("Id", IntegerType, true),
    StructField("MSSubClass", IntegerType, true),
    StructField("MSZoning", StringType, true),
    StructField("LotFrontage", IntegerType, true),
    StructField("LotArea", IntegerType, true),
    StructField("Street", StringType, true),
    StructField("Alley", StringType, true),
    StructField("LotShape", StringType, true),
    StructField("LandContour", StringType, true),
    StructField("Utilities", StringType, true),
    StructField("LotConfig", StringType, true),
    StructField("LandSlope", StringType, true),
    StructField("Neighborhood", StringType, true),
    StructField("Condition1", StringType, true),
    StructField("Condition2", StringType, true),
    StructField("BldgType", StringType, true),
    StructField("HouseStyle", StringType, true),
    StructField("OverallQual", IntegerType, true),
    StructField("OverallCond", IntegerType, true),
    StructField("YearBuilt", IntegerType, true),
    StructField("YearRemodAdd", IntegerType, true),
    StructField("RoofStyle", StringType, true),
    StructField("RoofMatl", StringType, true),
    StructField("Exterior1st", StringType, true),
    StructField("Exterior2nd", StringType, true),
    StructField("MasVnrType", StringType, true),
    StructField("MasVnrArea", StringType, true),
    StructField("ExterQual", StringType, true),
    StructField("ExterCond", StringType, true),
    StructField("Foundation", StringType, true),
    StructField("BsmtQual", StringType, true),
    StructField("BsmtCond", StringType, true),
    StructField("BsmtExposure", StringType, true),
    StructField("BsmtFinType1", StringType, true),
    StructField("BsmtFinSF1", IntegerType, true),
    StructField("BsmtFinType2", StringType, true),
    StructField("BsmtFinSF2", IntegerType, true),
    StructField("BsmtUnfSF", IntegerType, true),
    StructField("TotalBsmtSF", IntegerType, true),
    StructField("Heating", StringType, true),
    StructField("HeatingQC", StringType, true),
    StructField("CentralAir", StringType, true),
    StructField("Electrical", StringType, true),
    StructField("1stFlrSF", IntegerType, true),
    StructField("2ndFlrSF", IntegerType, true),
    StructField("LowQualFinSF", IntegerType, true),
    StructField("GrLivArea", IntegerType, true),
    StructField("BsmtFullBath", IntegerType, true),
    StructField("BsmtHalfBath", IntegerType, true),
    StructField("FullBath", IntegerType, true),
    StructField("HalfBath", IntegerType, true),
    StructField("BedroomAbvGr", IntegerType, true),
    StructField("KitchenAbvGr", IntegerType, true),
    StructField("KitchenQual", StringType, true),
    StructField("TotRmsAbvGrd", IntegerType, true),
    StructField("Functional", StringType, true),
    StructField("Fireplaces", IntegerType, true),
    StructField("FireplaceQu", StringType, true),
    StructField("GarageType", StringType, true),
    StructField("GarageYrBlt", IntegerType, true),
    StructField("GarageFinish", StringType, true),
    StructField("GarageCars", IntegerType, true),
    StructField("GarageArea", IntegerType, true),
    StructField("GarageQual", StringType, true),
    StructField("GarageCond", StringType, true),
    StructField("PavedDrive", StringType, true),
    StructField("WoodDeckSF", IntegerType, true),
    StructField("OpenPorchSF", IntegerType, true),
    StructField("EnclosedPorch", IntegerType, true),
    StructField("3SsnPorch", IntegerType, true),
    StructField("ScreenPorch", IntegerType, true),
    StructField("PoolArea", IntegerType, true),
    StructField("PoolQC", StringType, true),
    StructField("Fence", StringType, true),
    StructField("MiscFeature", StringType, true),
    StructField("MiscVal", IntegerType, true),
    StructField("MoSold", IntegerType, true),
    StructField("YrSold", IntegerType, true),
    StructField("SaleType", StringType, true),
    StructField("SaleCondition", StringType, true),
    StructField("SalePrice", IntegerType, true)
  )
  def trainingSchema: StructType = StructType(commonSchema)

  def testSchema: StructType = StructType(commonSchema.dropRight(1))
}
