This repository contains Hadoop Map-Reduce programs written in JAVA.

<h1>Inverse Web Link</h1>
    source  : InverseLink.java
    helper  : invLink.sh   
    input   : any .html file containing some <a href="..."></a> tags
<h1>Distributed grep</h1>
    source  : DistGep.java
    helper  : dGrep.sh
    input   : any text file
<h1>Matrix Multiplication(2-step,dense matrix)</h1>
    source  : MatrixMultiply.java  and MatStep2.java
    helper  : multi.sh
    input   : Give the matrices in a text file saved in names left.mat and right.mat
              For each of them , each line will contain a row and in the line , first put the line number
              and then put the elements of the row .The right matrix should be transposed.
              
              Sample input files given as left.mat and right.mat
<h1>Matrix Multiplication(1-step,sparse matrix)</h1>
    source  : MatrixMultiplySingleStep.java
    helper  : matrixMultiplySingleStep.sh
<h1>Matrix Vector Multiply</h1>
    source  : MatrixVectorMultiply.java
    helper  : matrixVectorMultiply.sh
<h1>Matrix Vector Multiply(Split in stripes)</h1>
    source  : MatrixVectorMultiplySplitted.java
    helper  : matrixVectorMultiplySplitted.sh

<hr><h3>To run this programs , follow the steps given below :</h3>

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
