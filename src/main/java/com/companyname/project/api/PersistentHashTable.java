package com.companyname.project.api;

import java.util.Hashtable;

import com.companyname.project.conf.Configuration;

public abstract class  PersistentHashTable  {
	
	public abstract void put(String key, String value);
	
	public abstract String get(String key);
	
	public abstract void recover();
	
	public abstract void init(Configuration conf) throws Exception; 
}
