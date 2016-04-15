hdfs dfs -rm -r /user/srijan/output*
javac -classpath hadoop-core-1.2.1.jar MovieRate.java 
jar cf mr.jar MovieRate*.class
hadoop jar mr.jar MovieRate /user/srijan/input/*.csv output
hadoop jar vm.jar VectorMean /user/srijan/output/p* output1
hdfs dfs -cat /user/srijan/output1/*

