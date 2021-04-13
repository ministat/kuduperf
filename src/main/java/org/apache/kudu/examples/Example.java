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

package org.apache.kudu.examples;

import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.ListTablesResponse;
import org.apache.kudu.examples.threading.StressExecutors;
import org.apache.log4j.BasicConfigurator;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kudu.client.KuduClient;

import static org.apache.kudu.examples.KuduOperations.*;
import static org.apache.kudu.examples.Utilities.convertExceptionMessage;

/*
 * A simple example of using the synchronous Kudu Java client to
 * - Create a table.
 * - Insert rows.
 * - Alter a table.
 * - Scan rows.
 * - Delete a table.
 */
public class Example {
  public static String RunInternal(int threadId,
                                   ExampleArguments eArgParser) {
    final String kuduMasters = eArgParser.kuduMasters;
    final String tableName = (eArgParser.tableName == null ?
            "java_example-" + System.currentTimeMillis() :
            eArgParser.tableName) + "_" + threadId;
    if (eArgParser.useKerberos &&
        eArgParser.keytab != null &&
        eArgParser.principalName != null) {
      try {
        System.out.println("Thread " + threadId + ": Use kerberos for " +
                eArgParser.principalName + " through " + eArgParser.keytab);
        UserGroupInformation.loginUserFromKeytab(eArgParser.principalName, eArgParser.keytab);
        final KuduClient secureClient = UserGroupInformation.getLoginUser().doAs(
                new PrivilegedExceptionAction<KuduClient>() {

                  @Override
                  public KuduClient run() throws Exception {
                    return new KuduClient.KuduClientBuilder(kuduMasters).build();
                  }
                }
        );
        Boolean result = UserGroupInformation.getLoginUser().doAs(
                new PrivilegedExceptionAction<Boolean>() {
                  public Boolean run() {
                    kuduTableTest(tableName, secureClient, eArgParser);
                    return true;
                  }
                }
        );
        System.out.println("Kudu table test: " + result);
      } catch (Exception e) {
        return convertExceptionMessage(e);
      }
    } else {
      KuduClient client = new KuduClient.KuduClientBuilder(kuduMasters).build();
      String rtn = kuduTableTest(tableName, client, eArgParser);
      return rtn;
    }
    return null;
  }

  public static void ListAllTables(ExampleArguments eArgParser) {
    final String kuduMasters = eArgParser.kuduMasters;
    StringBuffer sb = new StringBuffer();
    KuduClient client = new KuduClient.KuduClientBuilder(kuduMasters).build();
    try {
      ListTablesResponse resp = client.getTablesList();
      List<String> tables = resp.getTablesList();
      if (tables != null && !tables.isEmpty()) {
        for (String s : tables) {
          sb.append(s).append(System.lineSeparator());
        }
        System.out.println(sb.toString());
      } else {
        System.out.println("No tables");
      }
    } catch (KuduException e) {
      e.printStackTrace();
    }
  }

  public static void RemoveTable(ExampleArguments eArgParser) {
    final String kuduMasters = eArgParser.kuduMasters;
    if (eArgParser.tableName == null) {
      System.out.println("Missing tableName");
      return;
    }

    final String tableName = eArgParser.tableName;
    KuduClient client = new KuduClient.KuduClientBuilder(kuduMasters).build();
    try {
      client.deleteTable(tableName);
      System.out.println("Table " + tableName + " was removed");
    } catch (KuduException e) {
      e.printStackTrace();
    }
  }

  public class ThreadingKuduExecutor implements Callable<String>
  {
    private int _threadId;
    private ExampleArguments _eArgParser;

    public ThreadingKuduExecutor(int tid, ExampleArguments eArgParser) {
      _threadId = tid;
      _eArgParser = eArgParser;
    }

    @Override
    public String call() throws Exception {
      return RunInternal(_threadId, _eArgParser);
    }
  }

  public static void runThreading(ExampleArguments eArgParser) {
    ThreadPoolExecutor executorService = (ThreadPoolExecutor) Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors());
    List<Future<String>> resultList = new ArrayList<Future<String>>();

    Example example = new Example();
    for (int i = 0; i < eArgParser.threads; i++) {
      ThreadingKuduExecutor tke = example.new ThreadingKuduExecutor(i, eArgParser);
      Future<String> result = executorService.submit(tke);
      resultList.add(result);
    }
    executorService.shutdown();
    System.out.println("Tasks submit finished");
    long start = System.currentTimeMillis();
    for (Future<String> future : resultList) {
      try {
        System.out.println("Task Done is " + future.isDone() + ". Result is " + future.get());
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    long end = System.currentTimeMillis();
    System.out.println("All tests takes " + (end - start) + " ms");
  }

  public static void multiThreadStressTest(final ExampleArguments eArgParser) {
    StressExecutors se = new StressExecutors(eArgParser.threads, eArgParser.threads, eArgParser.duration);
    se.run((i) -> {
      long start = System.currentTimeMillis();
      String rtn = RunInternal(i, eArgParser);
      long end = System.currentTimeMillis();
      return i + " thread takes: " + (end - start) + "ms." +
              System.lineSeparator() +
              "------------------------------" + System.lineSeparator() +
              rtn +
              "------------------------------" + System.lineSeparator();
    }, (s) -> {
      System.out.println(s);
    });
  }

  public static void main(String[] args) {
    BasicConfigurator.configure();
    final ExampleArguments eArgParser = new ExampleArguments();
    if (!eArgParser.parseArgs(args)) {
      return;
    };
    if (eArgParser.kuduMasters == null) {
      System.out.println("No kudu masters");
      return;
    }
    if (eArgParser.removeTable) {
      RemoveTable(eArgParser);
      return;
    }
    if (eArgParser.listTable) {
      ListAllTables(eArgParser);
      return;
    }
    if (eArgParser.duration > 0) {
      multiThreadStressTest(eArgParser);
    } else {
      runThreading(eArgParser);
    }
  }
}
