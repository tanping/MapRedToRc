package com.yahoo.ccdi.fetl.ao.property;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.record.Buffer;

import com.yahoo.ccdi.fetl.GlobalConfVarNames;
import com.yahoo.ccdi.fetl.MetricsKeyType;
import com.yahoo.ccdi.fetl.MetricsValueType;
import com.yahoo.ccdi.fetl.SQLiteCachedMap;

public class ULTReaderMapper extends
// Mapper<Text, ULTRecordJT, MetricsKeyType, MetricsValueType> {
    Mapper<Vector<Object>, Vector<Object>, MetricsKeyType, MetricsValueType> {

  /* Mapping from spaceid to property id */
  protected SQLiteCachedMap sqliteCachedMap = null;
  protected HashSet<String> interestedProperIdSet = null;
  protected HashSet<String> interestedSpaceIdSet = null;
  static private String prevCookie = "";

  private static int lineNumber = 0;

  @Override
  public void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    Configuration jConf = context.getConfiguration();

    getInterestedPropertyIdSet();
    getInterestedSpaceIdSet();
    setupSqliteCachedMap(jConf);
  }

  @Override
  public void map(Vector<Object> key, Vector<Object> value, Context context)
      throws IOException, InterruptedException {

    //if (lineNumber++ < 6) {
      String bcookie = (String) value.get(0);
      String recordtype = (String) value.get(1);
      String adinfo = (String) value.get(2);
      String spaceid = (String) value.get(3);
      
      System.out.println("bcookie : " + bcookie + "\t recordtype : "+recordtype
          +"\t adinfo : = "+ adinfo + "\t spaceid = "+spaceid);
      // is page view -- recordtype is p
      long pv = recordtype.equals(GlobalConfVarNames.PV_RECORD) ? 1 : 0;
      // is ad clicks -- recordtype is c
      long ad_clicks = recordtype.equals(GlobalConfVarNames.AD_CLICK) ? 1 : 0;
      if (ad_clicks == 1L){
        context.getCounter(GlobalConfVarNames.ALL_PROPERTY_ID+"-m",
            GlobalConfVarNames.COUNTER_AC).increment(1L);
      }
      // is ad views -- recordtype is p and adinfo is not null
      if (adinfo == null) adinfo = "";
      long ad_views = ((recordtype.equals(GlobalConfVarNames.PV_RECORD))
          && (adinfo != null)) ? 1 : 0;
      if (ad_views == 1L){
        context.getCounter(GlobalConfVarNames.ALL_PROPERTY_ID+"-m",
            GlobalConfVarNames.COUNTER_AV).increment(1L);
      }
      
      // get property
      String propertyId = generatePropertyId(stripSpaceid(spaceid),
          sqliteCachedMap);

      if (interestedProperIdSet.contains(propertyId)) {
      //if (interestedSpaceIdSet.contains(spaceid)) {
        // is interested property
        MetricsKeyType metricsKey = new MetricsKeyType(bcookie, propertyId);
        MetricsValueType metricsValue = new MetricsValueType(pv, ad_clicks,
            ad_views);
        //System.out.println("Metrics key:["+bcookie+","+propertyId+". Metrics value:["+pv+", "+ad_clicks+", "+ad_views+"]");
        // emit per propertyId
        context.write(metricsKey, metricsValue);
      }
      
      MetricsValueType metricsValue = new MetricsValueType(pv, ad_clicks,
          ad_views);
      //System.out.println("Metrics key: "+ bcookie + ",000 Metrics value:["+pv+", "+ad_clicks+", "+ad_views+"]");
      // emit to all
      context.write(new MetricsKeyType(bcookie, "000"), metricsValue);
      
      if ((!bcookie.equals("")) && (bcookie != null)
          && (!bcookie.equals(prevCookie))) {
        prevCookie = bcookie;
        context.getCounter(GlobalConfVarNames.ALL_PROPERTY_ID+"-m",
            GlobalConfVarNames.COUNTER_BCOOKIE).increment(1L);
      }
    }
  //}

  /**
   * set up the sqliteCachedMap
   * 
   * @param jConf
   * @throws IOException
   */
  private void setupSqliteCachedMap(Configuration jConf) throws IOException {
    Path[] localFiles = DistributedCache.getLocalCacheFiles(jConf);

    for (int i = 0; i < localFiles.length; ++i) {

      if (localFiles[i].getName().indexOf("conformed_pty_mapping.db") != -1) {
        String dbFile = localFiles[i].toString();
        try {
          sqliteCachedMap = new SQLiteCachedMap(dbFile,
              GlobalConfVarNames.PTY_MAP_DEFAULT_CACHE_SIZE);
        } catch ( ClassNotFoundException e ) {
          e.printStackTrace();
        } catch ( SQLException e ) {
          e.printStackTrace();
        }
      }
    }

    Path[] localArchives = DistributedCache.getLocalCacheArchives(jConf);

    if (localArchives != null) {
      for (int i = 0; i < localArchives.length; ++i) {
        if (localArchives[i].getName().indexOf("conformed_pty_mapping.db") != -1) {
          String dbFileDir = localArchives[i].toString();
          String dbFile = dbFileDir
              + Path.SEPARATOR
              + localArchives[i].getName().substring(0,
                  localArchives[i].getName().length() - 4);

          try {
            sqliteCachedMap = new SQLiteCachedMap(dbFile,
                GlobalConfVarNames.PTY_MAP_DEFAULT_CACHE_SIZE);
          } catch ( ClassNotFoundException e ) {
            e.printStackTrace();
          } catch ( SQLException e ) {
            e.printStackTrace();
          }
        }
      }
    }
  }

  /**
   * get the interested property id set
   * 
   * @param jConf
   */
  private void getInterestedPropertyIdSet() {
    interestedProperIdSet = new HashSet<String>();
    interestedProperIdSet.add(GlobalConfVarNames.MAIL_PROPERTY_ID);
    interestedProperIdSet.add(GlobalConfVarNames.FRONT_PAGE_PROPERTY_ID);
    interestedProperIdSet.add(GlobalConfVarNames.NEWS_PROPERTY_ID);
    interestedProperIdSet.add(GlobalConfVarNames.FINANCE_PROPERTY_ID);
    interestedProperIdSet.add(GlobalConfVarNames.SEARCH_PROPERTY_ID);
    interestedProperIdSet.add(GlobalConfVarNames.SPORTS_PROPERTY_ID);
    interestedProperIdSet.add(GlobalConfVarNames.OMG_PROPERTY_ID);
  }

  /**
   * get the interested space id set
   * 
   * @param jConf
   */
  private void getInterestedSpaceIdSet() {
    interestedSpaceIdSet = new HashSet<String>();
    interestedSpaceIdSet.add(GlobalConfVarNames.MAIL_SPACE_ID);
    interestedSpaceIdSet.add(GlobalConfVarNames.FRONT_PAGE_SPACE_ID);
    interestedSpaceIdSet.add(GlobalConfVarNames.NEWS_SPACE_ID);
    interestedSpaceIdSet.add(GlobalConfVarNames.FINANCE_SPACE_ID);
    interestedSpaceIdSet.add(GlobalConfVarNames.SEARCH_SPACE_ID);
    interestedSpaceIdSet.add(GlobalConfVarNames.SPORTS_SPACE_ID);
    interestedSpaceIdSet.add(GlobalConfVarNames.OMG_SPACE_ID);
  }
  
  /**
   * Get spaceid from ULT record. Look it up from database and get its property
   * id.
   * 
   * @param val
   *          ULTRecordJT
   * @param cachedMap
   * @return property id
   */
  public static String generatePropertyId(String spaceid,
      SQLiteCachedMap cachedMap) {
    if ((spaceid != null) && (!spaceid.equals(""))) {
      try {
        String ptyString;

        if (cachedMap != null
            && (ptyString = cachedMap.lookup(spaceid)) != null) {
          return ptyString;
        }
      } catch ( UnsupportedEncodingException e1 ) {
        e1.printStackTrace();
      } catch ( SQLException e2 ) {
        e2.getStackTrace();
      }
    }
    return GlobalConfVarNames.UNKNOWN_PROPERTY_ID;
  }

  /**
   * Strip the prefix from spaceids.
   * 
   * @param spaceid
   *          the original spaceid.
   * @return The stripped version of the spaceid.
   */
  public static String stripSpaceid(String spaceid) {
    if (!spaceid.isEmpty() && spaceid.length() > 1 && spaceid.charAt(1) == '#') {
      return spaceid.substring(2);
    }

    return spaceid;
  }
}
