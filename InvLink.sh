hdfs dfs -rm /user/srijan/input/*
hdfs dfs -put TestPages/*.html /user/srijan/input/
hdfs dfs -rm -r /user/srijan/output*
hadoop jar il.jar InverseLink /user/srijan/input/*.html output
hadoop dfs -cat /user/srijan/output/*
