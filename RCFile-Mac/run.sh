find . -name \*class | xargs rm
rm *jar

javac -classpath lib/hadoop-0.20.2-core.jar:lib/hive-exec-0.8.0-SNAPSHOT.jar:lib/commons-cli-2.0-SNAPSHOT.jar:lib/commons-logging-1.0.4.jar com/yahoo/ccdi/fetl/*java

#javac -classpath /home/tanping/src/hadoop-0.20.2/hadoop-0.20.2-core.jar:/home/tanping/src/hadoop-0.18.3/lib/commons-cli-2.0-SNAPSHOT.jar:lib/hive-exec-0.6.0.jar com/yahoo/ccdi/fetl/*java

jar cvf ABF1RCPrinter.jar .

## Regular print from sequence to text
#$HADOOP_HOME/bin/hadoop fs -rmr /textoutput

#$HADOOP_HOME/bin/hadoop jar ABF1RCPrinter.jar com.yahoo.ccdi.fetl.ABF1Printer /input/part-00004-yapache-valid  /textoutput -m 4 
#$HADOOP_HOME/bin/hadoop fs -ls /textoutput

## MapRed to RC
$HADOOP_HOME/bin/hadoop fs -rmr /rcoutput

$HADOOP_HOME/bin/hadoop jar ABF1RCPrinter.jar com.yahoo.ccdi.fetl.ABF1RCPrinter /input/part-00004-yapache-valid  /rcoutput -m 2 
$HADOOP_HOME/bin/hadoop fs -ls /rcoutput

### convert from RC to Text
$HADOOP_HOME/bin/hadoop fs -rmr /rc2textoutput
$HADOOP_HOME/bin/hadoop jar ABF1RCPrinter.jar com.yahoo.ccdi.fetl.Rc2Text /rcoutput/part-00000 /rc2textoutput -m 2 -r 0

## copy output file to local
rm /tmp/part*
$HADOOP_HOME/bin/hadoop fs -get /rcoutput/part-00000 /tmp/part-rcoutput.txt
$HADOOP_HOME/bin/hadoop fs -get /rc2textoutput/part-00000 /tmp/part-rc2textoutput.txt

### list the output files
$HADOOP_HOME/bin/hadoop fs -ls /input
$HADOOP_HOME/bin/hadoop fs -ls /rcoutput
$HADOOP_HOME/bin/hadoop fs -ls /rc2textoutput


