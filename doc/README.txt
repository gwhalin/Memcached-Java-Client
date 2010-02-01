WARNING!

This version is compatible with legacy whalin version, so you can use it without 
re-compile your source code in most cases. Refer to COMPATIBLE.txt for detailed information. 

This release required Java 5 in order to work.  If you would like to degrade to 1.4
please feel free.


Known issues:
=============
1. you are always get null when value size is big for windows platform for the UDP protocol.
2. Out of memory when connections number is big. Try to specify more memory with -Xmx.

Reports:
============
Find the following reports in the directory.
1. Unit test report
   report/index.html
2. Code coverage report
   report/coverage.html
3. benchmark test report
   doc/benchmark.html
4. javadoc
   javadoc/index.html