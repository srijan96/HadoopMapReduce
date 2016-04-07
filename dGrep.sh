hdfs dfs -rm /user/srijan/input/*
hdfs dfs -put TestData/dg*.txt /user/srijan/input/
hdfs dfs -rm -r /user/srijan/output*
hadoop jar dgrep.jar DistGrep /user/srijan/input/dg*.txt output 
hadoop dfs -cat /user/srijan/output/*
