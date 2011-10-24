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

### Process large size of file 2G
hadoop jar ABF1RCPrinter.jar com.yahoo.ccdi.fetl.ABF1SEQPrinter /projects/FETL/Opportunity/201108310000/20110830/yahoo/part-00693 /user/tanping/seqoutput -cblock 128000000 -raw 128000000 -splitsize 2000000000

### Process all large size files
hadoop jar ABF1RCPrinter.jar com.yahoo.ccdi.fetl.ABF1RCPrinter /projects/FETL/Opportunity/201108310000/20110830/yahoo /user/tanping/rcoutput -cblock 1000000 -raw 134217728 -splitsize 0 
# Process one large size file with options 
hadoop jar ABF1RCPrinter.jar com.yahoo.ccdi.fetl.ABF1RCPrinter  hdfs://cobaltblue-nn1.blue.ygrid.yahoo.com/projects/FETL/Opportunity/201108310000/20110830/yahoo/part-00693 /user/tanping/rcoutput -cblock 1000000 -raw 134217728 -splitsize 2000000000

#### ETL Sequence to ETL Sequence large file
hadoop jar ABF1SEQPrinter.jar com.yahoo.ccdi.fetl.ABF1Seq2SeqPrinter hdfs://cobaltblue-nn1.blue.ygrid.yahoo.com/projects/FETL/Opportunity/201108310000/20110830/yahoo/part-00690 /user/tanping/seq2seqoutput -cblock 1000000 -raw 134217728 -splitsize 2000000000

### ETL Sequence to ETL No Key Sequence large file
hadoop jar ABF1SEQPrinter.jar com.yahoo.ccdi.fetl.ABF1SEQPrinter hdfs://cobaltblue-nn1.blue.ygrid.yahoo.com/projects/FETL/Opportunity/201108310000/20110830/yahoo/part-00691 /user/tanping/seqnokeyoutput -cblock 1000000 -raw 134217728 -splitsize 2000000000


### Seq => Opptunity Seq
hadoop jar ABF1RCPrinter.jar com.yahoo.ccdi.fetl.ABF1SEQPrinter /projects/FETL/tanping/20110825/yahoo/part-00692 /user/tanping/abf1seqprinteroutput





Test suite of JuteRC
#### Seq => Seq only 2 lines
hadoop jar ABF1RCPrinter.jar com.yahoo.ccdi.fetl.ABF1Seq2SeqPrinter /projects/FETL/tanping/20110825/yahoo/part-00692 /user/tanping/abf1seqprinteroutput

### Seq only 2 lines => Text
hadoop jar ABF1RCPrinter.jar com.yahoo.ccdi.fetl.ABF1Printer /user/tanping/abf1seqprinteroutput /user/tanping/textoutput

### Seq only 2 lines => Jute RC
 hadoop jar ABF1RCPrinter.jar com.yahoo.ccdi.fetl.Seq2Rc /user/tanping/abf1seqprinteroutput /user/tanping/seq2rcoutput

### JuteRc => Text 
hadoop jar ABF1RCPrinter.jar com.yahoo.ccdi.fetl.JuteRc2Text /user/tanping/seq2rcoutput /user/tanping/juterc2textoutput






#### Seq => JuteRc
 hadoop jar ABF1RCPrinter.jar com.yahoo.ccdi.fetl.Seq2Rc /projects/FETL/tanping/20110825/yahoo/part-00692 /user/tanping/seq2rcoutput

### Seq => text
 hadoop jar ABF1RCPrinter.jar com.yahoo.ccdi.fetl.ABF1Printer /projects/FETL/tanping/20110825/yahoo/part-00692 /user/tanping/seq2textoutput



### get Seq.gz file to local
hadoop fs -get /user/tanping/juterc2seqoutput /tmp/
gunzip /tmp/juterc2seqoutput/part-00000.gz
hadoop fs -put /tmp/juterc2seqoutput/part-00000 /user/tanping/juterc2seqoutput
hadoop fs -rm /user/tanping/juterc2seqoutput/part-00000.gz

### Seq => Text
hadoop jar ABF1RCPrinter.jar com.yahoo.ccdi.fetl.ABF1Printer /user/tanping/juterc2seqoutput/part-00000 /user/tanping/textoutput
