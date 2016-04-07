hdfs dfs -rm /user/srijan/input/*
hdfs dfs -put TestData/*.mat /user/srijan/input/
hdfs dfs -rm -r /user/srijan/output*
hadoop jar mm.jar MatrixMultiply /user/srijan/input/*.mat output
hadoop jar m2.jar MatStep2 /user/srijan/output/p* output1
hadoop dfs -cat /user/srijan/output1/*
