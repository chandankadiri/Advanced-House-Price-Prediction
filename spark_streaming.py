from pyspark import SparkContext                                                                                        
from pyspark.sql import SparkSession                                                                                    
from pyspark.streaming import StreamingContext                                                                          
from pyspark.streaming.kafka import KafkaUtils                                                                          
import house_model 
import json

def handle_rdd(rdd):                                                                                                    
    if not rdd.isEmpty():                                                                                               
        global ss                                                                                                       
        df = ss.createDataFrame(rdd)                                                
        df.show()                                                                                                       
        #df.write.saveAsTable(name='default.tweets', format='hive', mode='append')                                       

def predict_housesaleprice(row_rdd ):
    # Put RDD into a Dataframe
    df = ss.read.json(row_rdd)
    df.registerTempTable("temporary_table")
    df = ss.sql("""SELECT * FROM temporary_table""")
    #df.show()
    predicted = hackathon.predict(df)
    print(predicted)
    #return predicted

sc = SparkContext.getOrCreate()                                                                                     
ssc = StreamingContext(sc, 10)                                                                                           
                                                                                                                        
ss = SparkSession.builder.appName("hackathon").getOrCreate()                                                                                                  
                                                                                                                        
ss.sparkContext.setLogLevel('WARN')                                                                                     
                                                                                                                        
ks = KafkaUtils.createDirectStream(ssc, ['housesalepredictor'], {'metadata.broker.list': 'localhost:9092'})                       

#sid = SentimentIntensityAnalyzer()
lines = ks.map(lambda x: x[1])                                                                                          
lines.foreachRDD( lambda row_rdd: predict_housesaleprice(row_rdd) )
                                                                                                                        
ssc.start()                                                                                                             
ssc.awaitTermination()

# CREATE TABLE tweets (text STRING, words INT, length INT, text STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\|' STORED AS TEXTFILE;
