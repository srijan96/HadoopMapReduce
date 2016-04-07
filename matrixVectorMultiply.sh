hdfs dfs -rm /user/srijan/input/*
hdfs dfs -put TestData/matrix.mat /user/srijan/input/
hdfs dfs -rm -r /user/srijan/output*
hadoop jar mvm.jar MatrixVectorMultiply /user/srijan/input/matrix.mat output
hadoop dfs -cat /user/srijan/output/*
