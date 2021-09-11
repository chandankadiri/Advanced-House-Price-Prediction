from pyspark.sql import SparkSession, SQLContext, functions as F, DataFrame
from pyspark.ml.feature import StringIndexer, VectorAssembler, Imputer, VectorIndexer, Bucketizer, OneHotEncoderEstimator
from pyspark.ml.regression import LinearRegression, RandomForestRegressor, GBTRegressor
from pyspark.ml.pipeline import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
spark = SparkSession.builder.getOrCreate()
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
#%matplotlib inline
import json

def _cast_to_int(_sdf: DataFrame,col_list: list) -> DataFrame:
    for col in col_list:
        _sdf = _sdf.withColumn(col, _sdf[col].cast('int'))
    return _sdf

def train_model():
    PATH = '/files'
    sdf_train = spark.read.csv(f'{PATH}/train.csv', inferSchema=True, header=True)
    #sdf_test = spark.read.csv(f'{PATH}/test.csv', inferSchema=True, header=True)
    sdf_sample_submission = spark.read.csv(f'{PATH}/sample_submission.csv', 
                                       inferSchema=True, header=True)
    col_sample_submission = ['Id','SalePrice']
    drop_cols=['LotFrontage','MasVnrArea','GarageYrBlt']
    sdf_train= sdf_train.drop(*drop_cols)
    #sdf_test= sdf_test.drop(*drop_cols)
    str_features = []
    int_features = []
    for col in  sdf_train.dtypes:
        if col[1] == 'string':
            str_features += [col[0]]
        else:
            int_features += [col[0]]
#     print(col)
    print(f'str_features : {str_features}')
    print(f'int_features: {int_features}')
    # print(features)
    sdf_train_filter = sdf_train.select(int_features + str_features)
    int_features.remove('SalePrice')
    #sdf_test_filter = sdf_test.select(int_features + str_features)
    #sdf_test_typecast = _cast_to_int(sdf_test_filter, int_features)
    _stages = []
    str_indexer = [StringIndexer(inputCol=column, outputCol=f'{column}_StringIndexer', handleInvalid='keep') for column in str_features]
    _stages += str_indexer

    assembler_input = [f for f in int_features]
    assembler_input += [f'{column}_StringIndexer' for column  in str_features]
    feature_vector = VectorAssembler(inputCols=assembler_input, outputCol='features', handleInvalid = 'keep')
    _stages += [feature_vector]


    vect_indexer = VectorIndexer(inputCol='features', outputCol= 'features_indexed', handleInvalid = 'keep')
    _stages += [vect_indexer]
    RFR = RandomForestRegressor(featuresCol='features_indexed', labelCol='SalePrice', ) #0.18353
    _stages += [RFR]
    ml_pipeline = Pipeline(stages=_stages)
    model = ml_pipeline.fit(sdf_train_filter)
    print('model trained with randome forest')

    return model

model = train_model()

def predict(data):
    #import pdb;pdb.set_trace()
    if isinstance(data,dict):
        df = pd.DataFrame([json.loads(data)])
        sdf_test = spark.createDataFrame(df)
    else:
        sdf_test = data
    drop_cols=['LotFrontage','MasVnrArea','GarageYrBlt']
    sdf_test= sdf_test.drop(*drop_cols)
    str_features = []
    int_features = []
    for col in  sdf_test.dtypes:
        if col[1] == 'string':
            str_features += [col[0]]
        else:
            int_features += [col[0]]
    sdf_test_filter = sdf_test.select(int_features + str_features)
    sdf_test_typecast = _cast_to_int(sdf_test_filter, int_features)
    sdf_predict = model.transform(sdf_test_typecast)
    df = sdf_predict.withColumnRenamed('prediction','SalePrice').select('Id','SalePrice').toPandas()
    predicted = df.to_dict(orient='records')
    print(predicted)
    return predicted

if __name__ == "__main__":
    predict('''{"Id":1,"MSSubClass":60,"LotArea":8450,"OverallQual":7,"OverallCond":5,"YearBuilt":2003,"YearRemodAdd":2003,"BsmtFinSF1":706,"BsmtFinSF2":0,"BsmtUnfSF":150,"TotalBsmtSF":856,"1stFlrSF":856,"2ndFlrSF":854,"LowQualFinSF":0,"GrLivArea":1710,"BsmtFullBath":1,"BsmtHalfBath":0,"FullBath":2,"HalfBath":1,"BedroomAbvGr":3,"KitchenAbvGr":1,"TotRmsAbvGrd":8,"Fireplaces":0,"GarageCars":2,"GarageArea":548,"WoodDeckSF":0,"OpenPorchSF":61,"EnclosedPorch":0,"3SsnPorch":0,"ScreenPorch":0,"PoolArea":0,"MiscVal":0,"MoSold":2,"YrSold":2008,"MSZoning":"RL","Street":"Pave","Alley":"NA","LotShape":"Reg","LandContour":"Lvl","Utilities":"AllPub","LotConfig":"Inside","LandSlope":"Gtl","Neighborhood":"CollgCr","Condition1":"Norm","Condition2":"Norm","BldgType":"1Fam","HouseStyle":"2Story","RoofStyle":"Gable","RoofMatl":"CompShg","Exterior1st":"VinylSd","Exterior2nd":"VinylSd","MasVnrType":"BrkFace","ExterQual":"Gd","ExterCond":"TA","Foundation":"PConc","BsmtQual":"Gd","BsmtCond":"TA","BsmtExposure":"No","BsmtFinType1":"GLQ","BsmtFinType2":"Unf","Heating":"GasA","HeatingQC":"Ex","CentralAir":"Y","Electrical":"SBrkr","KitchenQual":"Gd","Functional":"Typ","FireplaceQu":"NA","GarageType":"Attchd","GarageFinish":"RFn","GarageQual":"TA","GarageCond":"TA","PavedDrive":"Y","PoolQC":"NA","Fence":"NA","MiscFeature":"NA","SaleType":"WD","SaleCondition":"Normal"}''')
