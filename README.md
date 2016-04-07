This repository contains Hadoop Map-Reduce programs written in JAVA.

To run this programs , follow the steps given below :

1> Compile the program

    javac -classpath < path to hadoop core jar file > < path to source file >
    
    example : javac -classpath hadoop-core-1.2.1.jar DistGrep.java 
    
2> Create a jar

    jar cf <name of jar> <name of .class files>
    
    example : jar cf dgrep.jar DistGrep*.class
    
3> Run the corresponding .sh file

    ./<name of .sh file>
    
    example : ./dGrep.sh
    before you run the BASH file for the first time , you have to provide it execute permission as follows,
      
      chmod +x dGrep.sh
