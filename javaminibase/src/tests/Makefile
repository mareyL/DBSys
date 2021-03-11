# modify the paths appropriately
JDKPATH = ~/Documents/jdk-11.0.4
LIBPATH = ./..
CLASSPATH = $(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

# test buffer manager

BMTest:BMTest.java
	$(JAVAC) TestDriver.java BMTest.java

bmtest: BMTest
	$(JAVA) tests.BMTest

# test disk manager

DBTest:DBTest.java
	$(JAVAC) TestDriver.java DBTest.java

dbtest: DBTest
	$(JAVA) tests.DBTest

# test heapfile 

HFTest:HFTest.java
	$(JAVAC) TestDriver.java HFTest.java

hftest: HFTest
	$(JAVA) tests.HFTest

# test btree

BTTest:BTTest.java
	$(JAVAC) TestDriver.java BTTest.java

bttest: BTTest
	$(JAVA) tests.BTTest

# test index

IndexTest:IndexTest.java
	$(JAVAC) TestDriver.java IndexTest.java

indextest: IndexTest
	$(JAVA) tests.IndexTest

# test join

JoinTest:JoinTest.java
	$(JAVAC) TestDriver.java JoinTest.java

jointest: JoinTest
	$(JAVA) tests.JoinTest

# test query1a

JoinTest2:JoinTest2.java
	$(JAVAC) TestDriver.java JoinTest2.java

jointest2: JoinTest2
	$(JAVA) tests.JoinTest2

# test query1b

JoinTest3:JoinTest3.java
	$(JAVAC) TestDriver.java JoinTest3.java

jointest3: JoinTest3
	$(JAVA) tests.JoinTest3

# test query2a

SelfJoinTest1:SelfJoinTest1.java
	$(JAVAC) TestDriver.java SelfJoinTest1.java

selfjointest1: SelfJoinTest1
	$(JAVA) tests.SelfJoinTest1

# test query2b

SelfJoinTest2:SelfJoinTest2.java
	$(JAVAC) TestDriver.java SelfJoinTest2.java

selfjointest2: SelfJoinTest2
	$(JAVA) tests.SelfJoinTest2

# test query2c

IEJoinTest:IEJoinTest.java
	$(JAVAC) TestDriver.java IEJoinTest.java

iejointest: IEJoinTest
	$(JAVA) tests.IEJoinTest


# test sort

SortTest:SortTest.java
	$(JAVAC) TestDriver.java SortTest.java

sorttest: SortTest
	$(JAVA) tests.SortTest

# test sortMerge

SMJoin:SM_JoinTest.java
	$(JAVAC) SM_JoinTest.java TestDriver.java

sortmerge: SMJoin
	$(JAVA) tests.SM_JoinTest
