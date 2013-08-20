CassandraTest 
=============

Basic performance tests for Cassandra.


Installation and Running
========================

The project is built and run using SBT ([Scala Build Tool](http://www.scala-sbt.org/))

1. Make sure that you have Scala and SBT installed

2. Make sure that you have [Cassandra](http://cassandra.apache.org/) installed on your local machine

2. Checkout the repository to a local directory

3. Compile by calling `sbt compile`

4. Run with `sbt run`

Different test sizes can be specified when running the program.


Sample Result
=============
    - Doing a simple test for 10,000 insertions.
    - Connected to cluster 'Test Cluster'.
    - Created keyspace 'profile_test'.
    - Created table 'mytable'.
    - Now inserting data...
    - Inserted       0 elements in    0.0 sec (0 elem/sec)
    - Inserted     500 elements in    0.5 sec (920 elem/sec)
    - Inserted   1,000 elements in    1.0 sec (1,013 elem/sec)
    - Inserted   1,500 elements in    1.3 sec (1,173 elem/sec)
    - Inserted   2,000 elements in    1.5 sec (1,298 elem/sec)
    - Inserted   2,500 elements in    2.1 sec (1,199 elem/sec)
    - Inserted   3,000 elements in    2.4 sec (1,258 elem/sec)
    - Inserted   3,500 elements in    2.7 sec (1,280 elem/sec)
    - Inserted   4,000 elements in    3.0 sec (1,324 elem/sec)
    - Inserted   4,500 elements in    3.4 sec (1,319 elem/sec)
    - Inserted   5,000 elements in    3.9 sec (1,291 elem/sec)
    - Inserted   5,500 elements in    4.2 sec (1,319 elem/sec)
    - Inserted   6,000 elements in    4.4 sec (1,359 elem/sec)
    - Inserted   6,500 elements in    4.7 sec (1,396 elem/sec)
    - Inserted   7,000 elements in    4.9 sec (1,431 elem/sec)
    - Inserted   7,500 elements in    5.1 sec (1,461 elem/sec)
    - Inserted   8,000 elements in    5.4 sec (1,488 elem/sec)
    - Inserted   8,500 elements in    5.7 sec (1,495 elem/sec)
    - Inserted   9,000 elements in    6.0 sec (1,495 elem/sec)
    - Inserted   9,500 elements in    6.3 sec (1,503 elem/sec)
    - Done inserting data. Total Time = 7 sec. Total elements = 10,000. Throughput = 1,506 elem/sec
    - Done inserting data. Total Time = 6 sec. Total elements = 10,000. Throughput = 1,628 elem/sec
    - Getting a few ids: 
	 102a940a-9eab-49db-a978-4810c2c9dca1
	 2dafc9ff-8cdd-4f82-b0d1-d96d1172e785
	 976120e9-7655-4e99-8503-884f5e912763
    - Writing the results into '2013-07-20_12:07:32__data.csv' (10,000 lines)
    - Here's a histogram of the results:
         0.1:          0 (  0.0%) | 
         0.2:          0 (  0.0%) | 
         0.3:      1,336 ( 13.4%) | *************
         0.4:      4,240 ( 42.4%) | ******************************************
         0.5:      2,227 ( 22.3%) | **********************
         0.6:        922 (  9.2%) | *********
         0.7:        433 (  4.3%) | ****
         0.8:        261 (  2.6%) | **
         0.9:        158 (  1.6%) | *
         1.0:         99 (  1.0%) | 
        10.0:        316 (  3.2%) | ***
        20.0:          1 (  0.0%) | 
    Infinity:          7 (  0.1%) | 
       TOTAL:     10,000 (100.0%) | ****************************************************************************************************

License
=======

MIT LICENSE

Copyright (C) 2013 Paulo Marques (pjp.marques@gmail.com)

Permission is hereby granted, free of charge, to any person obtaining a copy of 
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:
 
The above copyright notice and this permission notice shall be included in all 
copies or substantial portions of the Software.
 
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN 
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
