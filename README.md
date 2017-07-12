# Hadoop Relational Patterns

This is a set of simple example of hadoop implementaion on relational patterns.   
* Union
* Difference
* Intersection

## Usage   
Assuming environment variables are set as follows:   
```
export JAVA_HOME=/usr/java/default
export PATH=${JAVA_HOME}/bin:${PATH}
export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar
```

Take Union for example. Compile Union.java and create a jar:   
```
$ bin/hadoop com.sun.tools.javac.Main Union.java
$ jar cf wc.jar Union*.class
```

If you are new to hadoop, and have just set up the environment.   
Create HDFS file paths, assume your hadoop user is hduser:   
```
$ hadoop fs -mkdir -p /user/hduser
```

Upload the testcases from local to HDFS `union/input`:
```
$ hadoop fs -mkdir -p union/input
$ hadoop fs -put testcases/* union/input
```

Then run the Union program, download the result from HDFS to local:
```
$ hadoop jar union.jar Union union/input/tupleA union/input/tupleB union/output/
$ hadoop fs -get union/output ./
```

