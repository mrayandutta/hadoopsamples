$ hive --service hiveserver


You can connect to Hive in two modes. 
Through thrift server and embedded mode. B
To connect to the thrift sever,ensure that you are have started the hive thrift server by the following command.
$ hive --service hiveserver
To use the embedded mode you should add the hive/conf and the jars in hive/lib to your classpath.
It is to be noted that, use of the Thrift server is not thread safe as of now.

http://stackoverflow.com/questions/17694871/could-not-establish-connection-to-localhost10000-default-java-net-connectexcep