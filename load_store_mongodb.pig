--take data from a mongo database and insert into a new database
REGISTER drivers/mongodb-driver-3.0.4.jar;
REGISTER drivers/mongo-hadoop-core-1.5.0-SNAPSHOT.jar
REGISTER drivers/mongo-hadoop-pig-1.5.0-SNAPSHOT.jar

set default_parallel 5
set mapred.map.tasks.speculative.execution false
set mapred.reduce.tasks.speculative.execution false

a = LOAD 'mongodb://reportsUser:12345678@sandbox.hortonworks.com:27017/enron_mail.messages'
USING com.mongodb.hadoop.pig.MongoLoader('id, body, headers:[]', 'id');
b = limit a 100;
c = filter b by $1 is not null;
--dump c;

STORE c INTO 'mongodb://writesUser:12345678@sandbox.hortonworks.com:27017/enron_processed.messages'
USING com.mongodb.hadoop.pig.MongoInsertStorage('', '' );
