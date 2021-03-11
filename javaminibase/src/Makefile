JARFILES=bufmgr/*.class diskmgr/*.class global/*.class iterator/*.class\
         heap/*.class chainexception/*.class  btree/*.class index/*.class tests/*.class

JDKPATH = ~/Documents/jdk-11.0.4
LIBPATH = ./..
CLASSPATH = $(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)
	
test: 
	cd tests; make jointest
query1a: 
	cd tests; make jointest2
query1b: 
	cd tests; make jointest3
query2a: 
	cd tests; make selfjointest1
query2b: 
	cd tests; make selfjointest2
query2c: 
	cd tests; make iejointest

