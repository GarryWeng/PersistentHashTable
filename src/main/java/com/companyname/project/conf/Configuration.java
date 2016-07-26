package com.companyname.project.conf;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class Configuration {
  private HashMap<String,String> confMap = new HashMap<String,String>();
  
  public void set(String key, String value ){
    confMap.put(key, value);
  }
  
  public String get(String key){
    return confMap.get(key);
  }
  
  public String getTrimmed(String name) {
    String value = get(name);
    
    if (null == value) {
      return null;
    } else {
      return value.trim();
    }
  }
  
  public int getInt(String name, int defaultValue) {
    String valueString = getTrimmed(name);
    if (valueString == null)
      return defaultValue;
    String hexString = getHexDigits(valueString);
    if (hexString != null) {
      return Integer.parseInt(hexString, 16);
    }
    return Integer.parseInt(valueString);
  }
  
  private String getHexDigits(String value) {
    boolean negative = false;
    String str = value;
    String hexString = null;
    if (value.startsWith("-")) {
      negative = true;
      str = value.substring(1);
    }
    if (str.startsWith("0x") || str.startsWith("0X")) {
      hexString = str.substring(2);
      if (negative) {
        hexString = "-" + hexString;
      }
      return hexString;
    }
    return null;
  }
  
  public long getLong(String name, long defaultValue) {
    String valueString = getTrimmed(name);
    if (valueString == null)
      return defaultValue;
    String hexString = getHexDigits(valueString);
    if (hexString != null) {
      return Long.parseLong(hexString, 16);
    }
    return Long.parseLong(valueString);
  }
  
  public String get(String name, String defaultValue) {
    String result = null;
    result = get(name);
    if(result == null){
      result = defaultValue;
    }
    return result;
  }
  
  public static final String ZK_ADDRESS = "zk-address";
  
  public static final String ZK_TIMEOUT_MS = "zk-timeout-ms";
  public static final int DEFAULT_ZK_TIMEOUT_MS = 10000;
  
  public static final String ZK_NUM_RETRIES = "zk-num-retries";
  public static final int DEFAULT_ZK_NUM_RETRIES = 1000;
  
  public static final String ZK_RETRY_INTERVAL_MS = "zk-retry-interval-ms";
  public static final long DEFAULT_ZK_RETRY_INTERVAL_MS = 1000;
  
  public static final String ZK_STORE_ROOT_DIR = "zk-store.root-dir";
  
  public static final String DEFAULT_ZK_STATE_STORE_PARENT_PATH = "/store";
  
  public static final String ZK_HASHTABLE_STORE_DIR =  "zk-store.hashtable-dir";
  
  public static final String ZK_ACL =  "zk-acl";
  public static final String DEFAULT_ZK_ACL =  "world:anyone:rwcda"; 
  
  public static final String ZK_AUTH = "zk-auth";
}
