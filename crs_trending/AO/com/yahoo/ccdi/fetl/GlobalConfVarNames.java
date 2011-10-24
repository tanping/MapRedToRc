package com.yahoo.ccdi.fetl;

public class GlobalConfVarNames {
  public static final String SPACEID = "spaceid";
  public static final String RECORD_TYPE_KEY = "recordtype";
  public static final String AD_INFO_KEY = "adinfo";
  public static final String BCOOKIE_KEY = "bcookie";
  
  public static final String PV_RECORD = "p";
  public static final String AD_CLICK = "c";
  
  public static final String COUNTER_BCOOKIE = "bcookie";
  public static final String COUNTER_PV = "pv";
  public static final String COUNTER_AC = "ac";
  public static final String COUNTER_AV = "av";
  
  public static final String UNKNOWN_PROPERTY_ID = "133";
  public static final String ALL_PROPERTY_ID = "000";
  public static final String INTERESTED_PROPERTY_IDS = "interested_property_ids";
  public static final String MAIL_PROPERTY_ID = "158";
  public static final String FRONT_PAGE_PROPERTY_ID = "260";
  public static final String NEWS_PROPERTY_ID = "114";
  public static final String FINANCE_PROPERTY_ID = "125";
  public static final String SEARCH_PROPERTY_ID = "18025";
  public static final String SPORTS_PROPERTY_ID = "127";
  public static final String OMG_PROPERTY_ID = "703";
  
  public static final String ALL_PROPERTY = "ALL";
  public static final String MAIL_PROPERTY = "MAIL";
  public static final String FRONT_PAGE_PROPERTY = "FP";
  public static final String NEWS_PROPERTY = "NEWS";
  public static final String FINANCE_PROPERTY = "FINANCE";
  public static final String SEARCH_PROPERTY = "SEARCH";
  public static final String SPORTS_PROPERTY = "SPORTS";
  public static final String OMG_PROPERTY = "OMG";
  public static final String UNKNOWN_PROPERTY = "UNKNOWN_PROPERTY";

  public static final String MAIL_SPACE_ID = "3297502";
  public static final String FRONT_PAGE_SPACE_ID = "2716149";
  public static final String NEWS_SPACE_ID = "7645365";
  public static final String FINANCE_SPACE_ID = "2719353";
  public static final String SEARCH_SPACE_ID = "2716145";
  public static final String SPORTS_SPACE_ID = "  2716427";
  public static final String OMG_SPACE_ID = "15550340";

  public static final int ALL_PROPERTY_ID_INT = 0;
  public static final int NEWS_PROPERTY_ID_INT = 114;
  public static final int FINANCE_PROPERTY_ID_INT = 125;
  public static final int SPORTS_PROPERTY_ID_INT = 127;
  public static final int MAIL_PROPERTY_ID_INT = 158;
  public static final int FRONT_PAGE_PROPERTY_ID_INT = 260;
  public static final int OMG_PROPERTY_ID_INT = 703;
  public static final int SEARCH_PROPERTY_ID_INT = 18025;
  
  public static final String MAIL_DIR = "/MAIL/";
  public static final String FRONT_PAGE_DIR = "/FRONT_PAGE/";
  public static final String NEWS_DIR = "/NEWS/";
  public static final String FINANCE_DIR = "/FINANCE/";
  public static final String SEARCH_DIR = "/SEARCH/";
  public static final String SPORTS_DIR = "/SPORTS/";
  public static final String OMG_DIR = "/OMG/";
  public static final String ALL_DIR = "/ALL/";
  
  public static final int NUM_PROPERTY = 7;
  
  public static final int PTY_MAP_DEFAULT_CACHE_SIZE = 200000;
  
  // reducer assigned to process ALL records
  public static final int ALL_REDUCER_OFFSET = 128;
  // reducer assigned to process every property
  public static final int PROPERTY_REDUCER_OFFSET = 32;
  
  public static final int NUM_REDUCERS = 
    ALL_REDUCER_OFFSET + PROPERTY_REDUCER_OFFSET * NUM_PROPERTY;
  
  public static final String METRICS_SUMMARY = "/MetricsSummary";
}
