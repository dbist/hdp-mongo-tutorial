add jar hdfs://sandbox.hortonworks.com:8020/tmp/udfs/mongo-hadoop-hive-1.5.0-SNAPSHOT.jar;
add jar hdfs://sandbox.hortonworks.com:8020/tmp/udfs/mongo-hadoop-core-1.5.0-SNAPSHOT.jar;
add jar hdfs://sandbox.hortonworks.com:8020/tmp/udfs/mongodb-driver-3.0.4.jar;
DROP TABLE IF EXISTS bars;
CREATE EXTERNAL TABLE bars
(
objectid STRING,
    Symbol STRING,
    TS STRING,
    Day INT,
    Open DOUBLE,
    High DOUBLE,
    Low DOUBLE,
    Close DOUBLE,
    Volume INT
)
STORED BY 'com.mongodb.hadoop.hive.MongoStorageHandler'
WITH SERDEPROPERTIES('mongo.columns.mapping'='{"objectid":"_id",
 "Symbol":"Symbol", "TS":"Timestamp", "Day":"Day", "Open":"Open", "High":"High", "Low":"Low", "Close":"Close", "Volume":"Volume"}')
TBLPROPERTIES('mongo.uri'='mongodb://localhost:27017/marketdata.minibars');
