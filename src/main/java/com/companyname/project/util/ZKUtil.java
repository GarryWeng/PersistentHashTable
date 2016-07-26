package com.companyname.project.util;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.io.Files;



public class ZKUtil {
  
  /**
   * Parse ACL permission string, partially borrowed from
   * ZooKeeperMain private method
   * @throws Exception 
   */
  private static int getPermFromString(String permString) throws Exception {
    int perm = 0;
    for (int i = 0; i < permString.length(); i++) {
      char c = permString.charAt(i); 
      switch (c) {
      case 'r':
        perm |= ZooDefs.Perms.READ;
        break;
      case 'w':
        perm |= ZooDefs.Perms.WRITE;
        break;
      case 'c':
        perm |= ZooDefs.Perms.CREATE;
        break;
      case 'd':
        perm |= ZooDefs.Perms.DELETE;
        break;
      case 'a':
        perm |= ZooDefs.Perms.ADMIN;
        break;
      default:
        throw new Exception(
            "Invalid permission '" + c + "' in permission string '" +
            permString + "'");
      }
    }
    return perm;
  }

  /**
   * Helper method to remove a subset of permissions (remove) from a
   * given set (perms).
   * @param perms The permissions flag to remove from. Should be an OR of a
   *              some combination of {@link ZooDefs.Perms}
   * @param remove The permissions to be removed. Should be an OR of a
   *              some combination of {@link ZooDefs.Perms}
   * @return A permissions flag that is an OR of {@link ZooDefs.Perms}
   * present in perms and not present in remove
   */
  public static int removeSpecificPerms(int perms, int remove) {
    return perms ^ remove;
  }
  
  /**
   * An authentication token passed to ZooKeeper.addAuthInfo
   */
  public static class ZKAuthInfo {
    private final String scheme;
    private final byte[] auth;
    
    public ZKAuthInfo(String scheme, byte[] auth) {
      super();
      this.scheme = scheme;
      this.auth = auth;
    }

    public String getScheme() {
      return scheme;
    }

    public byte[] getAuth() {
      return auth;
    }
  }
  
  /**
   * Because ZK ACLs and authentication information may be secret,
   * allow the configuration values to be indirected through a file
   * by specifying the configuration as "@/path/to/file". If this
   * syntax is used, this function will return the contents of the file
   * as a String.
   * 
   * @param valInConf the value from the Configuration 
   * @return either the same value, or the contents of the referenced
   * file if the configured value starts with "@"
   * @throws IOException if the file cannot be read
   */
  public static String resolveConfIndirection(String valInConf)
      throws IOException {
    if (valInConf == null) return null;
    if (!valInConf.startsWith("@")) {
      return valInConf;
    }
    String path = valInConf.substring(1).trim();
    return Files.toString(new File(path), Charsets.UTF_8).trim();
  }
  
  /**
   * Parse a comma-separated list of authentication mechanisms. Each
   * such mechanism should be of the form 'scheme:auth' -- the same
   * syntax used for the 'addAuth' command in the ZK CLI.
   * 
   * @param authString the comma-separated auth mechanisms
   * @return a list of parsed authentications
   * @throws {@link BadAuthFormatException} if the auth format is invalid
   */
  public static List<ZKAuthInfo> parseAuth(String authString)
      throws Exception {
    List<ZKAuthInfo> ret = Lists.newArrayList();
    if (authString == null) {
      return ret;
    }

    List<String> authComps =
        Lists.newArrayList(Splitter.on(',').omitEmptyStrings().trimResults()
            .split(authString));

    for (String comp : authComps) {
      String parts[] = comp.split(":", 2);
      if (parts.length != 2) {
        throw new Exception("Auth '" + comp
            + "' not of expected form scheme:auth");
      }
      ret.add(new ZKAuthInfo(parts[0], parts[1].getBytes(Charsets.UTF_8)));
    }
    return ret;
  }
  
  
  /**
   * Parse comma separated list of ACL entries to secure generated nodes, e.g.
   * <code>sasl:hdfs/host1@MY.DOMAIN:cdrwa,sasl:hdfs/host2@MY.DOMAIN:cdrwa</code>
   *
   * @return ACL list
   * @throws {@link BadAclFormatException} if an ACL is invalid
   */
  public static List<ACL> parseACLs(String aclString) throws
      Exception {
    List<ACL> acl = Lists.newArrayList();
    if (aclString == null) {
      return acl;
    }
    
    List<String> aclComps = Lists.newArrayList(
        Splitter.on(',').omitEmptyStrings().trimResults()
        .split(aclString));
    for (String a : aclComps) {
      // from ZooKeeperMain private method
      int firstColon = a.indexOf(':');
      int lastColon = a.lastIndexOf(':');
      if (firstColon == -1 || lastColon == -1 || firstColon == lastColon) {
        throw new Exception(
            "ACL '" + a + "' not of expected form scheme:id:perm");
      }

      ACL newAcl = new ACL();
      newAcl.setId(new Id(a.substring(0, firstColon), a.substring(
          firstColon + 1, lastColon)));
      newAcl.setPerms(getPermFromString(a.substring(lastColon + 1)));
      acl.add(newAcl);
    }
    
    return acl;
  }
}
