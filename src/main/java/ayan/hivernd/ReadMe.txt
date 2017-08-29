$ hive --service hiveserver


You can connect to Hive in two modes. 
Through thrift server and embedded mode. B
To connect to the thrift sever,ensure that you are have started the hive thrift server by the following command.
$ hive --service hiveserver
To use the embedded mode you should add the hive/conf and the jars in hive/lib to your classpath.
It is to be noted that, use of the Thrift server is not thread safe as of now.

http://stackoverflow.com/questions/17694871/could-not-establish-connection-to-localhost10000-default-java-net-connectexcep

http://finraos.github.io/HiveQLUnit/
http://pig.apache.org/docs/r0.8.1/pigunit.html
http://www.metascale.com/resources/blogs/154-better-faster-and-simpler-pig-development-using-pigunit.html#.Vy7eM1V97IU
https://blog.codecentric.de/en/2014/05/mapreduce-testing-pigunit-junit/
https://github.com/klarna/HiveRunner
https://github.com/edwardcapriolo/hive_test