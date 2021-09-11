from kafka import KafkaProducer                                                                                         
from random import randint                                                                                              
from time import sleep                                                                                                  
import sys,json       
import pandas as pd
                                                                                                                        
BROKER = 'localhost:9092'                                                                                               
TOPIC = 'housesalepredictor'                                                                                                      
                                                                                                                        
test_file = 'test.csv'                                                                                     
#rows = open(test_file).read().splitlines()                                                                             
df = pd.read_csv(test_file)
print(df.shape)
rows = df.to_dict(orient='records')

try:                                                                                                                    
    p = KafkaProducer(bootstrap_servers=BROKER)                                                                         
except Exception as e:                                                                                                  
    print(f"ERROR --> {e}")                                                                                             
    sys.exit(1)                                                                                                        
                                                                                                                        
while True:                                                                                                             
    for row in rows:       
        row = json.dumps(row).encode('utf-8')
        print(row)
        p.send(TOPIC, value=row)   
    print('sleeping 10secs')
    sleep(5)
