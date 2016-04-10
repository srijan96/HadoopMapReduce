hdfs dfs -rm /user/srijan/input/*
hdfs dfs -put TestData/matrix*.mat2 /user/srijan/input/
hdfs dfs -rm -r /user/srijan/output*
hadoop jar mmss.jar MatrixMultiplySingleStep /user/srijan/input/matrix*.mat2 output 
hadoop dfs -cat /user/srijan/output/*
