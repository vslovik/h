## Vectors normalization in parallel

#### Hadoop Mapreduce toy application

Use **gradle** to compile, package and test application

Build project

    $ gradle build
    
Run application tests

    $ gradle test -i

Make a fat jar that includes scala, scalacheck, hadoop/mapreduce, scalatest libraries 

    $ gradle shadowJar

Run shell script docker-hadoop.sh to build and provision with puppet docker based hadoop cluster. 

    $ docker-hadoop.s --create <number of nodes>
    
Run matrix generation hadoop application to create a test data

    $ docker exec <cluster node> bash -c \
    "hadoop jar /matrixnorm-home/build/libs/org.unipi.matrixnorm-1.0-SNAPSHOT.jar \ 
    org.unipi.matrixgen.MatrixGenDriver <number of mapreduce tasks>  \
    <number of generated records per task> <output path>"

example: 

    $ docker exec 20171208111653r15633_matrixnorm_1 bash -c \
    "hadoop jar /matrixnorm-home/build/libs/org.unipi.matrixnorm-1.0-SNAPSHOT-all.jar \ 
    org.unipi.matrixgen.MatrixGenDriver 1 1000 gen"

To see generated files execute

    $ docker exec <cluster node>  bash -c "hadoop fs -ls <output path>"

console output example:
    
    Found 2 items
    -rw-r--r--   3 hadoop hadoop          0 2017-12-08 10:31 genn/_SUCCESS
    -rw-r--r--   3 hadoop hadoop      72527 2017-12-08 10:31 genn/part-m-00000
    
Inspect generated records

    $ docker exec <cluster node>  bash -c "hadoop fs -cat <output path>/part-m-00000"

console output example:

    3	2	18.202	24.9998	14.93	17.0676	12.7798	12.8403
    2	2	0.2935	24.2654	17.3495	60.8572
    2	3	53.6227	69.6355	82.0388	87.2193	11.5522	13.4696
    4	4	19.6024	5.0906	60.8055	22.6151	3.6612	12.5976	26.418	36.9408	47.6779	59.3008	59.6499	29.7212	4.1653	58.9121	12.1722	23.4723
    ...

Each string represents a matrix in serialized form: first field is a number of rows, second 
field is a number of columns, followed by tab separated values (read row by row from first to last column) 

Run vector normalization application

    $ docker exec <cluster node> bash -c \
    "hadoop jar /matrixnorm-home/build/libs/org.unipi.matrixnorm-1.0-SNAPSHOT-all.jar \ 
    org.unipi.matrixgen.HadoopMatrixNorm <input path> <output path>"
    
example:  

    $ docker exec 20171208111653r15633_matrixnorm_1 bash -c \
    "hadoop jar /matrixnorm-home/build/libs/org.unipi.matrixnorm-1.0-SNAPSHOT-all.jar \ 
    org.unipi.matrixgen.HadoopMatrixNorm gen out"
 
Inspect normalised vectors (matrix order in output corresponds to their order in input)

    $ docker exec <cluster node>  bash -c "hadoop fs -cat <output path>/part-r-00000"
       
console output example:

    3	2	1.0	1.0	0.3966	0.3509	0.0	0.005
    2	2	0.0	0.3958	0.6904	1.0
    2	3	0.6135	0.7977	0.9404	1.0	0.1295	0.1516
    4	4	0.2221	0.0552	0.6961	0.2568	0.0387	0.1415	0.3005	0.4216	0.5451	0.6788	0.6828	0.3385	0.0445	0.6744	0.1367	0.2667
    ...
   
   
## Matrix p-norm computation in parallel  

Generate test sparse matrix

    $ org.unipi.matrixrowgen.MatrixRowGenDriver <number of map tasks> <number of matrix rows per task> <number of matrix columns> <output path>

example:

    $ org.unipi.matrixrowgen.MatrixRowGenDriver 2 10000 100 rows

Run matrix p-norm application

    $ org.unipi.matrixpnorm.MatrixPNorm <input path> <output path> <p: double number>

example: 

    $ org.unipi.matrixpnorm.MatrixPNorm rows mpnorm 2.0

Run Naive Frobenius norm application

    $ org.unipi.matrixpnorm.MatrixNaiveFrobeniusNorm <input path> <output path>

example: 

    $ org.unipi.matrixpnorm.MatrixNaiveFrobeniusNorm rows mnfn

Run DimSum Frobenius norm application in two steps:

1. Pre-calculate column euclidean norms 

    $ org.unipi.matrixpnorm.MatrixColumnsMagnitudes <input path> <output path>

example:

    $ org.unipi.matrixpnorm.MatrixColumnsMagnitudes rows colnorms

2. Run DimSum Frobenius norm application

    $ org.unipi.matrixpnorm.MatrixDimSumFrobeniusNorm <columns norm input path> <matrix input path> <output path>

example:

    $ org.unipi.matrixpnorm.MatrixDimSumFrobeniusNorm colnorms rows dimsum  
