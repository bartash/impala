// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.impala.util;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTH_TO_LOCAL;

import com.google.common.collect.Iterables;
import com.google.common.io.Files;

import org.apache.hadoop.conf.Configuration;
import org.apache.impala.authorization.User;
import org.apache.impala.common.ByteUnits;
import org.apache.impala.common.InternalException;
import org.apache.impala.common.RuntimeEnv;
import org.apache.impala.thrift.TErrorCode;
import org.apache.impala.thrift.TPoolConfig;
import org.apache.impala.thrift.TResolveRequestPoolParams;
import org.apache.impala.thrift.TResolveRequestPoolResult;
import org.apache.impala.yarn.server.resourcemanager.scheduler.fair.
    AllocationConfigurationException;
import org.apache.impala.yarn.server.resourcemanager.scheduler.fair.
    AllocationFileLoaderService;
import org.apache.impala.yarn.server.resourcemanager.scheduler.fair.QueuePlacementPolicy;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for the user to pool resolution, authorization, and getting configuration
 * parameters via {@link RequestPoolService}. Sets a configuration file and ensures the
 * appropriate user to pool resolution, authentication, and pool configs are returned.
 * This also tests that updating the files after startup causes them to be reloaded and
 * the updated values are returned.
 * TODO: Move tests to C++ to test the API that's actually used.
 */
public class TestRequestPoolService {
  // Pool definitions and includes memory resource limits, copied to a temporary file
  private static final String ALLOCATION_FILE = "fair-scheduler-test.xml";

  // A second allocation file which overwrites the temporary file to check for changes.
  private static final String ALLOCATION_FILE_MODIFIED = "fair-scheduler-test2.xml";
  private static final String ALLOCATION_FILE_EXTRA = "fair-scheduler-test3.xml";
  private static final String ALLOCATION_FILE_EMPTY = "fair-scheduler-empty.xml";
  private static final String ALLOCATION_FILE_GROUP_RULE = "fair-scheduler-group-rule.xml";

  // Contains per-pool configurations for maximum number of running queries and queued
  // requests.
  private static final String LLAMA_CONFIG_FILE = "llama-site-test.xml";

  // A second Llama config which overwrites the temporary file to check for changes.
  private static final String LLAMA_CONFIG_FILE_MODIFIED = "llama-site-test2.xml";
  private static final String LLAMA_CONFIG_FILE_EMPTY = "llama-site-empty.xml";

  // Set the file check interval to something short so we don't have to wait long after
  // changing the file.
  private static final long CHECK_INTERVAL_MS = 100L;

  // Temp folder where the config files are copied so we can modify them in place.
  // The JUnit @Rule creates and removes the temp folder between every test.
  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private RequestPoolService poolService_;
  private File allocationConfFile_;
  private File llamaConfFile_;

  /**
   * Creates the poolService_ with the specified configuration.
   * @param allocationFile The file on the classpath of the allocation conf.
   * @param llamaConfFile The file on the classpath of the Llama conf. May be null to
   *                      create a RequestPoolService with no llama-conf.xml as it is
   *                      not required.
   */
  private void createPoolService(String allocationFile, String llamaConfFile)
      throws Exception {
    allocationConfFile_ = tempFolder.newFile("fair-scheduler-temp-file.xml");
    Files.copy(getClasspathFile(allocationFile), allocationConfFile_);

    String llamaConfPath = null;
    if (llamaConfFile != null) {
      llamaConfFile_ = tempFolder.newFile("llama-conf-temp-file.xml");
      Files.copy(getClasspathFile(llamaConfFile), llamaConfFile_);
      llamaConfPath = llamaConfFile_.getAbsolutePath();
    }
    poolService_ = RequestPoolService.getInstance(
        allocationConfFile_.getAbsolutePath(), llamaConfPath, /* isTest */ true);

    // Lower the wait times on the AllocationFileLoaderService and RequestPoolService so
    // the test doesn't have to wait very long to test that file changes are reloaded.
    Field f = AllocationFileLoaderService.class.getDeclaredField("reloadIntervalMs");
    f.setAccessible(true);
    f.set(poolService_.allocLoader_, CHECK_INTERVAL_MS);
    if (llamaConfFile != null) {
      poolService_.confWatcher_.setCheckIntervalMs(CHECK_INTERVAL_MS);
    }
    poolService_.start();
    // Make sure that the Hadoop configuration from classpath is used for the underlying
    // QueuePlacementPolicy.
    QueuePlacementPolicy policy = poolService_.getAllocationConfig().getPlacementPolicy();
    Configuration conf = policy.getConf();
    Assert.assertTrue(conf.getBoolean("impala.core-site.overridden", false));
  }

  @BeforeClass
  public static void setUpClass() throws Exception {
    RuntimeEnv.INSTANCE.setTestEnv(true);
    User.setRulesForTesting(
        new Configuration().get(HADOOP_SECURITY_AUTH_TO_LOCAL, "DEFAULT"));
  }

  @AfterClass
  public static void cleanUpClass() {
    RuntimeEnv.INSTANCE.reset();
  }

  @After
  public void cleanUp() throws Exception {
    if (poolService_ != null) poolService_.stop();
  }

  /**
   * Returns a {@link File} for the file on the classpath.
   */
  private File getClasspathFile(String filename) throws URISyntaxException {
    return new File(getClass().getClassLoader().getResource(filename).toURI());
  }

  @Test
  public void testPoolResolution() throws Exception {
    createPoolService(ALLOCATION_FILE, LLAMA_CONFIG_FILE);
    Assert.assertEquals("root.queueA", poolService_.assignToPool("root.queueA", "userA"));
    Assert.assertNull(poolService_.assignToPool("nonexistentQueue", "userA"));
  }

  @Test
  public void testResolvePrincipalName() throws Exception {
    // Tests that we can resolve user names that are Kerberos principals/LDAP users.
    createPoolService(ALLOCATION_FILE, LLAMA_CONFIG_FILE);
    TResolveRequestPoolResult result = poolService_.resolveRequestPool(
        new TResolveRequestPoolParams("userA@abc.com", "root.queueA"));
    Assert.assertEquals(TErrorCode.OK, result.getStatus().getStatus_code());
    Assert.assertEquals("root.queueA", result.getResolved_pool());

    result = poolService_.resolveRequestPool(
        new TResolveRequestPoolParams("userA/a.qualified.domain@abc.com", "root.queueA"));
    Assert.assertEquals(TErrorCode.OK, result.getStatus().getStatus_code());
    Assert.assertEquals("root.queueA", result.getResolved_pool());
  }

  @Test
  public void testUserNoGroupsError() throws Exception {
    // Test fix for IMPALA-922: "Return helpful errors with Yarn group rules"
    createPoolService(ALLOCATION_FILE_GROUP_RULE, LLAMA_CONFIG_FILE);
    TResolveRequestPoolResult result = poolService_.resolveRequestPool(
        new TResolveRequestPoolParams("userA", "root.NOT_A_POOL"));
    Assert.assertEquals(false, result.isSetResolved_pool());
    Assert.assertEquals(false, result.isSetHas_access());
    Assert.assertEquals(TErrorCode.INTERNAL_ERROR, result.getStatus().getStatus_code());

    String expectedMessage = "Failed to resolve user 'userA' to a pool while " +
    "evaluating the 'primaryGroup' or 'secondaryGroup' queue placement rules because " +
    "no groups were found for the user. This is likely because the user does not " +
    "exist on the local operating system.";
    Assert.assertEquals(expectedMessage,
        Iterables.getOnlyElement(result.getStatus().getError_msgs()));
  }

  @Test
  public void testPoolAcls() throws Exception {
    createPoolService(ALLOCATION_FILE, LLAMA_CONFIG_FILE);
    Assert.assertTrue(poolService_.hasAccess("root.queueA", "userA"));
    Assert.assertTrue(poolService_.hasAccess("root.queueB", "userB"));
    Assert.assertFalse(poolService_.hasAccess("root.queueB", "userA"));
    Assert.assertTrue(poolService_.hasAccess("root.queueB", "root"));
    // Test that comma separated users are parsed correctly.
    Assert.assertTrue(poolService_.hasAccess("root.queueD", "userA"));
    Assert.assertTrue(poolService_.hasAccess("root.queueD", "userB"));
    Assert.assertFalse(poolService_.hasAccess("root.queueD", "userZ"));
  }

  @Test
  public void testPoolLimitConfigs() throws Exception {
    createPoolService(ALLOCATION_FILE, LLAMA_CONFIG_FILE);
    Map<String, Integer> rootUserQueryLimits = new HashMap<>();
    rootUserQueryLimits.put("userB", 6);
    rootUserQueryLimits.put("*", 10);
    Map<String, Integer> rootGroupQueryLimits = new HashMap<>();
    rootGroupQueryLimits.put("group3", 5);
    checkPoolConfigResult("root", 15, 50, -1, 30000L, "mem_limit=1024m",
        rootUserQueryLimits, rootGroupQueryLimits);
    checkPoolConfigResult("root.queueA", 10, 30, 1024 * ByteUnits.MEGABYTE,
        10000L, "mem_limit=1024m,query_timeout_s=10");
    checkPoolConfigResult("root.queueB", 5, 10, -1, 30000L, "mem_limit=1024m");
    checkPoolConfigResult("root.queueC", 5, 10, 1024 * ByteUnits.MEGABYTE, 30000L,
        "mem_limit=1024m", 1000, 10, false, 8, 8, null, null);

    Map<String, Integer> queueDUserQueryLimits = new HashMap<>();
    queueDUserQueryLimits.put("userA", 2);
    queueDUserQueryLimits.put("userF", 2);
    queueDUserQueryLimits.put("userG", 101);
    queueDUserQueryLimits.put("*", 3);
    Map<String, Integer> queueDGroupQueryLimits = new HashMap<>();
    queueDGroupQueryLimits.put("group1", 1);
    queueDGroupQueryLimits.put("group2", 1);

    checkPoolConfigResult("root.queueD", 5, 10, -1, 30000L, "mem_limit=1024m",
        queueDUserQueryLimits, queueDGroupQueryLimits);

  }

  @Test
  public void testDefaultConfigs() throws Exception {
    createPoolService(ALLOCATION_FILE_EMPTY, LLAMA_CONFIG_FILE_EMPTY);
    Assert.assertEquals("root.userA", poolService_.assignToPool("", "userA"));
    Assert.assertTrue(poolService_.hasAccess("root.userA", "userA"));
    checkPoolConfigResult("root", -1, 200, -1, null, "", 0, 0, true, 0, 0, null, null);
  }

  @Ignore("IMPALA-4868") @Test
  public void testUpdatingConfigs() throws Exception {
    // Tests updating the config files and then checking the pool resolution, ACLs, and
    // pool limit configs. This tests all three together rather than separating into
    // separate test cases because we updateConfigFiles() will end up waiting around 7
    // seconds, so this helps cut down on the total test execution time.
    // A one second pause is necessary to ensure the file timestamps are unique if the
    // test gets here within one second.
    createPoolService(ALLOCATION_FILE, LLAMA_CONFIG_FILE);
    Thread.sleep(1000L);
    Files.copy(getClasspathFile(ALLOCATION_FILE_MODIFIED), allocationConfFile_);
    Files.copy(getClasspathFile(LLAMA_CONFIG_FILE_MODIFIED), llamaConfFile_);

    // Need to wait for the YARN AllocationFileLoaderService (for the
    // allocationConfFile_) as well as the FileWatchService (for the llamaConfFile_). If
    // the system is busy this may take even longer, so we need to try a few times.
    Thread.sleep(CHECK_INTERVAL_MS + AllocationFileLoaderService.ALLOC_RELOAD_WAIT_MS);

    int numAttempts = 20;
    while (true) {
      try {
        checkModifiedConfigResults();
        break;
      } catch (AssertionError e) {
        if (numAttempts == 0) throw e;
        --numAttempts;
        Thread.sleep(1000L);
      }
    }
  }

  @Test
  public void testModifiedConfigs() throws Exception {
    // Tests the results are the same as testUpdatingConfigs() as when we create the
    // pool service with the same modified configs initially (i.e. not updating).
    createPoolService(ALLOCATION_FILE_MODIFIED, LLAMA_CONFIG_FILE_MODIFIED);
    checkModifiedConfigResults();
  }

  /**
   * Validate reading user and group quotas
   */
  @Test
  public void testReadUserGroupQuotas() throws Exception {
    createPoolService(ALLOCATION_FILE_EXTRA, null);
    TPoolConfig rootConfig = poolService_.getPoolConfig("root");
    Map<String, Integer> rootUserExpected = new HashMap<String, Integer>() {
      {
        put("*", 8);
        put("howard", 4);
      }
    };
    Assert.assertEquals(rootUserExpected, rootConfig.user_query_limits);
    Map<String, Integer> rootGroupExpected = new HashMap<String, Integer>() {
      { put("support", 6); }
    };
    Assert.assertEquals(rootGroupExpected, rootConfig.group_query_limits);

    TPoolConfig smallConfig = poolService_.getPoolConfig("root.group-set-small");
    Map<String, Integer> smallUserExpected = new HashMap<String, Integer>() {
      {
        put("*", 1);
        put("alice", 4);
      }
    };
    Assert.assertEquals(smallUserExpected, smallConfig.user_query_limits);
    Map<String, Integer> smallGroupExpected = new HashMap<String, Integer>() {
      {
        put("support", 5);
        put("dev", 5);
        put("it", 2);
      }
    };
    Assert.assertEquals(smallGroupExpected, smallConfig.group_query_limits);

    TPoolConfig largeConfig = poolService_.getPoolConfig("root.group-set-large");
    Map<String, Integer> largeUserExpected = new HashMap<String, Integer>() {
      {
        put("*", 1);
        put("alice", 4);
        put("claire", 3);
      }
    };
    Assert.assertEquals(largeUserExpected, largeConfig.user_query_limits);
    Map<String, Integer> largeGroupExpected = new HashMap<String, Integer>() {
      {
        put("support", 1);
        put("dev", 2);
      }
    };
    Assert.assertEquals(largeGroupExpected, largeConfig.group_query_limits);
  }

  // Test pool resolution
  @Test
  public void testNullLlamaSite() throws Exception {
    createPoolService(ALLOCATION_FILE_MODIFIED, null);

    // Test pool resolution
    Assert.assertEquals("root.queueA", poolService_.assignToPool("queueA", "userA"));
    Assert.assertNull(poolService_.assignToPool("queueX", "userA"));
    Assert.assertEquals("root.queueC", poolService_.assignToPool("queueC", "userA"));

    // Test pool ACLs
    Assert.assertTrue(poolService_.hasAccess("root.queueA", "userA"));
    Assert.assertTrue(poolService_.hasAccess("root.queueB", "userB"));
    Assert.assertTrue(poolService_.hasAccess("root.queueB", "userA"));
    Assert.assertFalse(poolService_.hasAccess("root.queueC", "userA"));
    Assert.assertTrue(poolService_.hasAccess("root.queueC", "root"));
    Assert.assertTrue(poolService_.hasAccess("root.queueD", "userA"));

    // Test pool limits
    Map<String, Integer> rootQueryLimits = new HashMap<>();
    Map<String, Integer> rootGroupLimits = new HashMap<>();
    rootQueryLimits.put("userD", 2);
    checkPoolConfigResult(
        "root", -1, 200, -1, null, "", rootQueryLimits, rootGroupLimits);
    checkPoolConfigResult("root.queueA", -1, 200, 100000 * ByteUnits.MEGABYTE);
    checkPoolConfigResult("root.queueB", -1, 200, -1);
    checkPoolConfigResult("root.queueC", -1, 200, 128 * ByteUnits.MEGABYTE);

    Map<String, Integer> queueEUserQueryLimits = new HashMap<>();
    queueEUserQueryLimits.put("userA", 3);
    queueEUserQueryLimits.put("userG", 3);
    queueEUserQueryLimits.put("userH", 0);
    queueEUserQueryLimits.put("*", 1);
    Map<String, Integer> queueEGroupQueryLimits = new HashMap<>();
    queueEGroupQueryLimits.put("group1", 2);
    queueEGroupQueryLimits.put("group2", 2);

    checkPoolConfigResult("root.queueE", -1, 200, -1, null, "",
        queueEUserQueryLimits, queueEGroupQueryLimits);
  }

  @Test
  public void testNewLimitParsing() {
    String xmlString = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        +
        "<person>\n" +
        "    <name>John Doe</name>\n" +
        "    <age>30</age>\n" +
        "</person>";

    try {
      // Create a DocumentBuilderFactory instance
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();

      // Create a DocumentBuilder instance
      DocumentBuilder builder = factory.newDocumentBuilder();


      // Parse the XML string into a Document object
      Document document = builder.parse(new java.io.ByteArrayInputStream(xmlString.getBytes()));

      // Get the root element
      Element rootElement = document.getDocumentElement();


    } catch (Exception e) {
      Assert.fail("caught " +e);
    }
  }

  /**
   * Unit test for  AllocationFileLoaderService.addQueryLimits().
   */
  @Test
  public void testLimitsParsing() throws AllocationConfigurationException {
    Map<String, Map<String, Integer>> allLimits = new HashMap<>();
    String QUEUE1 = "queue1";
    String QUEUE2 = "queue2";
    String QUEUE3 = "queue3";
    AllocationFileLoaderService.addQueryLimits(allLimits, QUEUE1, "user1 1");
    AllocationFileLoaderService.addQueryLimits(allLimits, QUEUE1, " user2     2 ");
    AllocationFileLoaderService.addQueryLimits(allLimits, QUEUE1, "* 2");
    AllocationFileLoaderService.addQueryLimits(allLimits, QUEUE2, "user1 12 ");

    Map<String, Integer> queue1 = allLimits.get(QUEUE1);
    Map<String, Integer> queue2 = allLimits.get(QUEUE2);
    Map<String, Integer> queue3 = allLimits.get(QUEUE3);
    Assert.assertEquals(1,(long) queue1.get("user1"));
    Assert.assertEquals(2,(long) queue1.get("user2"));
    Assert.assertEquals(12,(long) queue2.get("user1"));
    Assert.assertNull(queue3);

    allLimits = new HashMap<>();
    AllocationFileLoaderService.addQueryLimits(allLimits, QUEUE1, "user1 1");
    try {
      AllocationFileLoaderService.addQueryLimits(allLimits, QUEUE1, "user1 2");
      Assert.fail("should have got exception");
    } catch (AllocationConfigurationException e) {
      Assert.assertTrue(e.getMessage().contains("Duplicate value"));
    }

    allLimits = new HashMap<>();
    try {
      AllocationFileLoaderService.addQueryLimits(allLimits, QUEUE1, "user1 xxx");
      Assert.fail("should have got exception");
    } catch (AllocationConfigurationException e) {
      Assert.assertTrue(e.getMessage().contains("Cannot parse"));
    }

    allLimits = new HashMap<>();
    try {
      AllocationFileLoaderService.addQueryLimits(allLimits, QUEUE1, "user1=xxx");
      Assert.fail("should have got exception");
    } catch (AllocationConfigurationException e) {
      Assert.assertTrue(e.getMessage().contains("name and number"));
    }
  }

  private void checkModifiedConfigResults()
      throws InternalException, IOException {
    // Test pool resolution: now there's a queueC
    Assert.assertEquals("root.queueA", poolService_.assignToPool("queueA", "userA"));
    Assert.assertNull(poolService_.assignToPool("queueX", "userA"));
    Assert.assertEquals("root.queueC", poolService_.assignToPool("queueC", "userA"));

    // Test pool ACL changes
    Assert.assertTrue(poolService_.hasAccess("root.queueA", "userA"));
    Assert.assertTrue(poolService_.hasAccess("root.queueB", "userB"));
    Assert.assertTrue(poolService_.hasAccess("root.queueB", "userA"));
    Assert.assertFalse(poolService_.hasAccess("root.queueC", "userA"));
    Assert.assertTrue(poolService_.hasAccess("root.queueC", "root"));

    // Test pool limit changes
    Map<String, Integer> rootQueryLimits = new HashMap<>();
    Map<String, Integer> rootGroupLimits = new HashMap<>();
    rootQueryLimits.put("userD", 2);
    checkPoolConfigResult(
        "root", 15, 100, -1, 30000L, "", rootQueryLimits, rootGroupLimits);
    // not_a_valid_option=foo.bar gets filtered out when parsing the query options on
    // the backend, but it should be observed coming from the test file here.
    checkPoolConfigResult("root.queueA", 1, 30, 100000 * ByteUnits.MEGABYTE, 50L,
        "mem_limit=128m,query_timeout_s=5,not_a_valid_option=foo.bar");
    checkPoolConfigResult("root.queueB", 5, 10, -1, 600000L, "");
    checkPoolConfigResult("root.queueC", 10, 30, 128 * ByteUnits.MEGABYTE, 30000L,
        "mem_limit=2048m,query_timeout_s=60");
  }

  /**
   * Helper method to verify the per-pool limits.
   */
  private void checkPoolConfigResult(String pool, long expectedMaxRequests,
      long expectedMaxQueued, long expectedMaxMem, Long expectedQueueTimeoutMs,
      String expectedQueryOptions, long max_query_mem_limit, long min_query_mem_limit,
      boolean clamp_mem_limit_query_option, long max_query_cpu_core_per_node_limit,
      long max_query_cpu_core_coordinator_limit, Map<String, Integer> userQueryLimits,
      Map<String, Integer> groupQueryLimits) {
    TPoolConfig expectedResult = new TPoolConfig();
    expectedResult.setMax_requests(expectedMaxRequests);
    expectedResult.setMax_queued(expectedMaxQueued);
    expectedResult.setMax_mem_resources(expectedMaxMem);
    expectedResult.setMax_query_mem_limit(max_query_mem_limit);
    expectedResult.setMin_query_mem_limit(min_query_mem_limit);
    expectedResult.setClamp_mem_limit_query_option(clamp_mem_limit_query_option);
    expectedResult.setMax_query_cpu_core_per_node_limit(
        max_query_cpu_core_per_node_limit);
    expectedResult.setMax_query_cpu_core_coordinator_limit(
        max_query_cpu_core_coordinator_limit);
    if (expectedQueueTimeoutMs != null) {
      expectedResult.setQueue_timeout_ms(expectedQueueTimeoutMs);
    }
    if (expectedQueryOptions != null) {
      expectedResult.setDefault_query_options(expectedQueryOptions);
    }
    expectedResult.setUser_query_limits(
        userQueryLimits != null ? userQueryLimits : Collections.emptyMap());
    expectedResult.setGroup_query_limits(
        groupQueryLimits != null ? groupQueryLimits : Collections.emptyMap());
    TPoolConfig poolConfig = poolService_.getPoolConfig(pool);
    Assert.assertEquals(
        "Unexpected config values for pool " + pool, expectedResult, poolConfig);
  }

  private void checkPoolConfigResult(String pool, long expectedMaxRequests,
      long expectedMaxQueued, long expectedMaxMem, Long expectedQueueTimeoutMs,
      String expectedQueryOptions) {
    checkPoolConfigResult( pool,  expectedMaxRequests,
     expectedMaxQueued,  expectedMaxMem,  expectedQueueTimeoutMs,
         expectedQueryOptions, Collections.emptyMap(), Collections.emptyMap());
  }

  private void checkPoolConfigResult(String pool, long expectedMaxRequests,
      long expectedMaxQueued, long expectedMaxMem, Long expectedQueueTimeoutMs,
      String expectedQueryOptions, Map<String, Integer> userQueryLimits,
      Map<String, Integer> groupQueryLimits) {
    checkPoolConfigResult(pool, expectedMaxRequests, expectedMaxQueued, expectedMaxMem,
        expectedQueueTimeoutMs, expectedQueryOptions, 0, 0, true, 0, 0,
        userQueryLimits, groupQueryLimits);
  }

  private void checkPoolConfigResult(String pool, long expectedMaxRequests,
      long expectedMaxQueued, long expectedMaxMemUsage) {
    checkPoolConfigResult(pool, expectedMaxRequests, expectedMaxQueued,
        expectedMaxMemUsage, null, "");
  }
}