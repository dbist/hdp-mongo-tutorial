# set up parameters for reading from MongoDB via Hadoop input format
config = {"mongo.input.uri": "mongodb://localhost:27017/marketdata.minibars"}
inputFormatClassName = "com.mongodb.hadoop.MongoInputFormat"
# these values worked but others might as well
keyClassName = "org.apache.hadoop.io.Text"
valueClassName = "org.apache.hadoop.io.MapWritable"

# read the 1-minute bars from MongoDB into Spark RDD format
minBarRawRDD = sc.newAPIHadoopRDD(inputFormatClassName, keyClassName, valueClassName, None, None, config)

# configuration for output to MongoDB
config["mongo.output.uri"] = "mongodb://localhost:27017/marketdata.fiveminutebars"
outputFormatClassName = "com.mongodb.hadoop.MongoOutputFormat"

# takes the verbose raw structure (with extra metadata) and strips down to just the pricing data
minBarRDD = minBarRawRDD.values()

minBarRDD.saveAsTextFile("hdfs://sandbox.hortonworks.com:8020/user/root/spark-mongo-output3")
