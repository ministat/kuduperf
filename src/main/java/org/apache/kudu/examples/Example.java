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

import io.prometheus.client.*;
import io.prometheus.client.exporter.*;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
  public static final int CONSUMER_QUEUE_LEN = 1024;
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

  public static void sendPushgateway(final ExampleArguments eArgParser,
                                     String label,
                                     long value) throws IOException {
    String url = eArgParser.prometheus_endpoint;
    if (url == null) {
      return;
    }
    String hostname = InetAddress.getLocalHost().getHostName();
    CollectorRegistry registry = new CollectorRegistry();
    Gauge guage = Gauge.build("kudu_stress_metrics", "Kudu stress test metrics")
            .labelNames("node", "label")
            .create();
    guage.labels(hostname, label).set(value);
    guage.register(registry);
    PushGateway pg = new PushGateway(url);
    Map<String, String> groupingKey = new HashMap<String, String>();
    groupingKey.put("instance", "kudu_instance");
    pg.pushAdd(registry, "kudu_job", groupingKey);
  }

  public static class PrometheusItem {
    public String label;
    public long value;
    public boolean end;
  }

  public static class  PrometheusPusherConsumer implements Runnable {
    private BlockingQueue<PrometheusItem> _queue;
    private CollectorRegistry _registry;
    private String _hostname;
    private PushGateway _pushgateway;
    private Map<String, String> _groupingKey;

    public PrometheusPusherConsumer(
            ExampleArguments eArgParser,
            BlockingQueue<PrometheusItem> q) {
      _pushgateway = new PushGateway(eArgParser.prometheus_endpoint);
      _queue = q;
      try {
        _hostname = InetAddress.getLocalHost().getHostName();
      } catch (UnknownHostException e) {
        e.printStackTrace();
      }
      _registry = new CollectorRegistry();
      _groupingKey = new HashMap<String, String>();
      _groupingKey.put("instance", "kudu_instance");
    }

    @Override
    public void run() {
      while (true) {
        try {
          PrometheusItem item = _queue.take();
          if (item.end) {
            // a flag is used to break the loop
            break;
          }
          _registry.clear();
          Gauge guage = Gauge.build("kudu_stress_metrics", "Kudu stress test metrics")
                  .labelNames("node", "label")
                  .create();
          guage.labels(_hostname, item.label).set(item.value);
          guage.register(_registry);
          _pushgateway.pushAdd(_registry, "kudu_job", _groupingKey);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
  }

  public static void multiThreadStressTest(final ExampleArguments eArgParser,
                                           BlockingQueue<PrometheusItem> queue) {
    StressExecutors se = new StressExecutors(eArgParser.threads, eArgParser.threads, eArgParser.duration);
    se.run((i) -> {
      long start = System.currentTimeMillis();
      String rtn = RunInternal(i, eArgParser);
      long end = System.currentTimeMillis();
      long duration = end - start;
      try {
        if (queue != null) {
          PrometheusItem item = new PrometheusItem();
          item.label = "thread_" + i;
          item.value = duration;
          item.end = false;
          queue.put(item);
        } else {
          sendPushgateway(eArgParser, "thread_" + i, duration);
        }
      } catch (Exception e) {
        System.out.println(convertExceptionMessage(e));
      }

      return i + " thread takes: " + duration + "ms." +
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
      BlockingQueue<PrometheusItem> queue = null;
      PrometheusPusherConsumer consumer = null;
      Thread t = null;
      try {
        if (eArgParser.prometheus_endpoint != null) {
          queue = new ArrayBlockingQueue<>(CONSUMER_QUEUE_LEN);
          consumer = new PrometheusPusherConsumer(eArgParser, queue);
        }
        if (consumer != null) {
          t = new Thread(consumer);
          t.start();
        }
        multiThreadStressTest(eArgParser, queue);
      } finally {
        // stop the thread
        if (t != null) {
          try {
            PrometheusItem item = new PrometheusItem();
            item.end = true;
            queue.put(item);
            t.join();
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      }

    } else {
      runThreading(eArgParser);
    }
  }
}
