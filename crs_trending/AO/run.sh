javac -classpath lib/hadoop-0.20.2-core.jar:lib/commons-cli-1.2.jar:lib/commons-cli-2.0-SNAPSHOT.jar:lib/fetl_sequence_projector.jar:lib/fetl_base_feed.jar:lib/sdsProcessing.jar:lib/sqlitejdbc-v054.jar:lib/fetl_sequence_projector.jar:lib/fetl_hadoop_io.jar com/yahoo/ccdi/fetl/*java com/yahoo/ccdi/fetl/ao/*java com/yahoo/ccdi/fetl/opptunity/*java

jar cvf AOCompare.jar .

hadoop jar AOCompare.jar com.yahoo.ccdi.fetl.ao.ULTReaderDriver -files hdfs://cobaltblue-nn1.blue.ygrid.yahoo.com/user/tanping/conformed_pty_mapping.db -Dinput=AO/part-00002 -Doutput=/user/tanping/ultoutput 


hadoop jar AOCompare.jar com.yahoo.ccdi.fetl.opptunity.OpportunityReaderDriver -Dinput=/projects/FETL/Opportunity2to1/201110162000/aoh/aoh/part-00517 -Doutput=/user/tanping/opptunityoutput

## Data 6 lines of AO data
/user/tanping/AOLShort

ABF1FieldGenerator#generatePropertyId

ABF1MapReduceBase.java
protected SQLiteCachedMap sqliteCachedMap = null;
ABF1Mapper.java

## run Opportunity AOHourly B-Cookie Count:
  ### Always on Cobalt-Blue:

hadoop jar AOCompare.jar com.yahoo.ccdi.fetl.ao.cookie.CookieCounterDriver -Dinput=/projects/FETL/Opportunity2to1/201110162000/aoh/aoh -Doutput=/user/tanping/oppoutput/BCookie-P -Dmapred.job.queue.name=audience_fetl -Dprojectedfields=bcookie:String,type:String,adinfo:String,spaceid:String

## run AOH FET1.x data P - Bcookie Count 
  ### on Nitro-Blue:
hadoop jar AOCompare.jar com.yahoo.ccdi.fetl.ao.cookie.ULTReaderDriver -Dinput=/data/SDS/data/ads_octopus_minibatch/2011101620/p -Doutput=/user/tanping/aooutput/BCookie-P -Dmapred.job.queue.name=unfunded -Dprojectedfields=bcookie:String,recordtype:String,adinfo:String,spaceid:String 

  ### on Cobalt-Blue:
hadoop jar AOCompare.jar com.yahoo.ccdi.fetl.ao.cookie.CookieCounterDriver -Dinput=/user/tanping/2011101620/p -Doutput=/user/tanping/aooutput/BCookie-P -Dmapred.job.queue.name=audience_fetl -Dprojectedfields=bcookie:String,recordtype:String,adinfo:String,spaceid:String



### AO data on Colba
hadoop jar AOCompare.jar com.yahoo.ccdi.fetl.ao.property.ULTReaderDriver -files hdfs://cobaltblue-nn1.blue.ygrid.yahoo.com/user/tanping/conformed_pty_mapping.db -Dinput=/user/tanping/2011101620/p/part-0008* -Doutput=/user/tanping/aooutput/BCookie-P -Dmapred.job.queue.name=audience_fetl -Dprojectedfields=bcookie:String,recordtype:String,adinfo:String,spaceid:String

hadoop jar AOCompare.jar com.yahoo.ccdi.fetl.ao.cookie.CookieCounterDriver -files hdfs://cobaltblue-nn1.blue.ygrid.yahoo.com/user/tanping/conformed_pty_mapping.db -Dinput=/user/tanping/2011101620/p/part-0008* -Doutput=/user/tanping/aooutput-2 -Dmapred.job.queue.name=audience_fetl -Dprojectedfields=bcookie:String,recordtype:String,adinfo:String,spaceid:String

### Opportunity Data on Cobal
hadoop jar AOCompare.jar com.yahoo.ccdi.fetl.ao.property.ULTReaderDriver -files hdfs://cobaltblue-nn1.blue.ygrid.yahoo.com/user/tanping/conformed_pty_mapping.db -Dinput=/projects/FETL/Opportunity2to1/201110162000/aoh/aoh -Doutput=/user/tanping/oppoutput/BCookie-P -Dmapred.job.queue.name=audience_fetl -Dprojectedfields=bcookie:String,type:String,adinfo:String,spaceid:String
