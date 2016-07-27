package com.companyname.project.project_main;

import com.companyname.project.api.PersistentHashTable;
import com.companyname.project.conf.Configuration;
import com.companyname.project.zkstore.PersistentHashTableOnZk;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class AppTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( AppTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testApp()
    {
      
      PersistentHashTable hashtable = new PersistentHashTableOnZk();
      Configuration conf = new Configuration();
      conf.set(Configuration.ZK_ADDRESS, "127.0.0.1:2181");
      conf.set(Configuration.ZK_NUM_RETRIES, "10");
      try{
        hashtable.init(conf);
        hashtable.put("testkey", "testvalue");
        hashtable = new PersistentHashTableOnZk();
        hashtable.init(conf);
        hashtable.recover();
        assertTrue( hashtable.get("testkey").equals("testvalue"));
      } catch (Exception e){
        assertTrue(false);
      }
    }
}
