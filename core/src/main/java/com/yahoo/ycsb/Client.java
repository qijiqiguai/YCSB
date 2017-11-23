/**
 * Copyright (c) 2010-2016 Yahoo! Inc., 2017 YCSB contributors All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb;

import com.yahoo.ycsb.target.DB;
import com.yahoo.ycsb.measurements.Measurements;
import com.yahoo.ycsb.measurements.exporter.MeasurementsExporter;
import com.yahoo.ycsb.measurements.exporter.TextMeasurementsExporter;
import com.yahoo.ycsb.target.DBFactory;
import com.yahoo.ycsb.target.UnknownDBException;
import com.yahoo.ycsb.workloads.Workload;
import com.yahoo.ycsb.workloads.WorkloadException;
import org.apache.htrace.core.HTraceConfiguration;
import org.apache.htrace.core.TraceScope;
import org.apache.htrace.core.Tracer;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;

/**
 * Main class for executing YCSB.
 */
public final class Client {
  private Client() {
    //not used
  }

  public static final String DEFAULT_RECORD_COUNT = "0";

  /**
   * The target number of operations to perform.
   */
  public static final String OPERATION_COUNT_PROPERTY = "operationCount";

  /**
   * The number of records to load into the database initially.
   */
  public static final String RECORD_COUNT_PROPERTY = "recordCount";

  /**
   * Indicates how many inserts to do if less than recordCount.
   * Useful for partitioning the load among multiple servers if the client is the bottleneck.
   * Additionally workloads should support the "insertStart" property which tells them which record to start at.
   */
  public static final String INSERT_COUNT_PROPERTY = "insertCount";

  /**
   * The workload class to be loaded.
   */
  public static final String WORKLOAD_PROPERTY = "workload";

  /**
   * The database class to be used.
   */
  public static final String DB_PROPERTY = "db";

  /**
   * The exporter class to be used. The default is
   * com.yahoo.ycsb.measurements.exporter.TextMeasurementsExporter.
   */
  public static final String EXPORTER_PROPERTY = "exporter";

  /**
   * If set to the path of a file, YCSB will write all output to this file
   * instead of STDOUT.
   */
  public static final String EXPORT_FILE_PROPERTY = "exportFile";

  /**
   * The number of YCSB client threads to run.
   */
  public static final String THREAD_COUNT_PROPERTY = "threadCount";

  /**
   * Target number of operations per second.
   */
  public static final String TARGET_PROPERTY = "target";

  /**
   * The maximum amount of time (in seconds) for which the benchmark will be run.
   */
  public static final String MAX_EXECUTION_TIME = "maxExecutionTime";

  /**
   * Whether or not this is the transaction phase (run) or not (load).
   */
  public static final String DO_TRANSACTIONS_PROPERTY = "doTransactions";

  /**
   * Whether or not to show status during run.
   */
  public static final String STATUS_PROPERTY = "status";

  /**
   * Use label for status (e.g. to label one experiment out of a whole batch).
   */
  public static final String LABEL_PROPERTY = "label";

  /**
   * An optional thread used to track progress and measure JVM stats.
   */
  private static StatusThread statusthread = null;

  // HTrace integration related constants.
  /**
   * All keys for configuring the tracing system start with this prefix.
   */
  private static final String HTRACE_KEY_PREFIX = "htrace.";
  private static final String CLIENT_WORKLOAD_INIT_SPAN = "Client#workload_init";
  private static final String CLIENT_INIT_SPAN = "Client#init";
  private static final String CLIENT_WORKLOAD_SPAN = "Client#workload";
  private static final String CLIENT_CLEANUP_SPAN = "Client#cleanup";
  private static final String CLIENT_EXPORT_MEASUREMENTS_SPAN = "Client#export_measurements";

  public static void usageMessage() {
    System.out.println("Usage: java com.yahoo.ycsb.Client [options]");
    System.out.println("Options:");
    System.out.println("  -threads n: execute using n threads (default: 1) - can also be specified as the \n" +
        "        'threadCount' property using -p");
    System.out.println("  -target n: attempt to do n operations per second (default: unlimited) - can also\n" +
        "       be specified as the 'target' property using -p");
    System.out.println("  -load:  run the loading phase of the workload");
    System.out.println("  -t:  run the transactions phase of the workload (default)");
    System.out.println("  -db dbName: specify the name of the DB to use (default: com.yahoo.ycsb.BasicDB) - \n" +
        "        can also be specified as the 'db' property using -p");
    System.out.println("  -P propertyFile: load properties from the given file. Multiple files can");
    System.out.println("           be specified, and will be processed in the order specified");
    System.out.println("  -p name=value:  specify a property to be passed to the DB and workloads;");
    System.out.println("          multiple properties can be specified, and override any");
    System.out.println("          values in the propertyFile");
    System.out.println("  -s:  show status during run (default: no status)");
    System.out.println("  -l label:  use label for status (e.g. to label one experiment out of a whole batch)");
    System.out.println("");
    System.out.println("Required properties:");
    System.out.println("  " + WORKLOAD_PROPERTY + ": the name of the workload class to use (e.g. " +
        "com.yahoo.ycsb.workloads.CoreWorkload)");
    System.out.println("");
    System.out.println("To run the load phase from multiple servers, start a separate client on each; additionally,");
    System.out.println("use the 'insertCount' and 'insertStart' properties to divide up the records " +
        "to be inserted");
  }

  public static boolean checkRequiredProperties(Properties props) {
    if (props.getProperty(WORKLOAD_PROPERTY) == null) {
      System.out.println("Missing property: " + WORKLOAD_PROPERTY);
      return false;
    }

    return true;
  }


  /**
   * Exports the measurements to either sysOut or a file using the exporter loaded from conf.
   *
   * FileExporter or System.out
   * DefinedExporter or TextMeasurementsExporter
   *
   * Export overall runtime | throughput | JVM Status(GC, MEM) | Sys Load
   *
   * @throws IOException Either failed to write to output stream or failed to close it.
   */
  private static void exportMeasurements(Properties props, int opCount, long runtime) throws IOException {
    MeasurementsExporter exporter = null;
    try {
      // if no destination file is provided the results will be written to stdout
      OutputStream out;
      String exportFile = props.getProperty(EXPORT_FILE_PROPERTY);
      if (exportFile == null) {
        out = System.out;
      } else {
        out = new FileOutputStream(exportFile);
      }

      // if no exporter is provided the default text one will be used
      String exporterStr = props.getProperty(EXPORTER_PROPERTY,
          "com.yahoo.ycsb.measurements.exporter.TextMeasurementsExporter");
      try {
        exporter = (MeasurementsExporter) Class.forName(exporterStr).getConstructor(OutputStream.class)
            .newInstance(out);
      } catch (Exception e) {
        System.err.println("Could not find exporter " + exporterStr + ", will use default text reporter.");
        e.printStackTrace();
        exporter = new TextMeasurementsExporter(out);
      }

      exporter.write("OVERALL", "RunTime(ms)", runtime);
      double throughput = 1000.0 * (opCount) / (runtime);
      exporter.write("OVERALL", "Throughput(ops/sec)", throughput);

      if (statusthread!=null && statusthread.trackJVMStats()) {
        final Map<String, Long[]> gcs = Utils.getGCStatst();
        long totalGCCount = 0;
        long totalGCTime = 0;
        for (final Entry<String, Long[]> entry : gcs.entrySet()) {
          exporter.write("TOTAL_GCS_" + entry.getKey(), "Count", entry.getValue()[0]);
          exporter.write("TOTAL_GC_TIME_" + entry.getKey(), "Time(ms)", entry.getValue()[1]);
          exporter.write("TOTAL_GC_TIME_%_" + entry.getKey(), "Time(%)",
              ((double) entry.getValue()[1] / runtime) * (double) 100);
          totalGCCount += entry.getValue()[0];
          totalGCTime += entry.getValue()[1];
        }
        exporter.write("TOTAL_GCs", "Count", totalGCCount);
        exporter.write("TOTAL_GC_TIME", "Time(ms)", totalGCTime);
        exporter.write("TOTAL_GC_TIME_%", "Time(%)", ((double) totalGCTime / runtime) * (double) 100);

        exporter.write("MAX_MEM_USED", "MBs", statusthread.getMaxUsedMem());
        exporter.write("MIN_MEM_USED", "MBs", statusthread.getMinUsedMem());
        exporter.write("MAX_THREADS", "Count", statusthread.getMaxThreads());
        exporter.write("MIN_THREADS", "Count", statusthread.getMinThreads());
        exporter.write("MAX_SYS_LOAD_AVG", "Load", statusthread.getMaxLoadAvg());
        exporter.write("MIN_SYS_LOAD_AVG", "Load", statusthread.getMinLoadAvg());
      }

      Measurements.getMeasurements().exportMeasurements(exporter);
    } finally {
      if (exporter != null) {
        exporter.close();
      }
    }
  }

  @SuppressWarnings("unchecked")
  public static void main(String[] args) {
    Properties props = parseArguments(args);

    boolean status = Boolean.valueOf(props.getProperty(STATUS_PROPERTY, String.valueOf(false)));
    String label = props.getProperty(LABEL_PROPERTY, "");

    long maxExecutionTime = Integer.parseInt(props.getProperty(MAX_EXECUTION_TIME, "0"));

    //get number of threads, target and db
    int threadCount = Integer.parseInt(props.getProperty(THREAD_COUNT_PROPERTY, "1"));
    String dbName = props.getProperty(DB_PROPERTY, "com.yahoo.ycsb.BasicDB");

    // Target throughput
    int target = Integer.parseInt(props.getProperty(TARGET_PROPERTY, "0"));
    //compute the target throughput for each thread
    double targetPerThreadPerms = -1;
    if (target > 0) {
      double targetPerThread = ((double) target) / ((double) threadCount);
      targetPerThreadPerms = targetPerThread / 1000.0;
    }


    // show a warning message that creating the workload is taking a while
    Thread warningThread = setupWarningThread();
    warningThread.start();

    Measurements.setProperties(props);

    Workload workload = getWorkload(props);

    final Tracer tracer = getTracer(props, workload);

    initWorkload(props, warningThread, workload, tracer);

    System.err.println("Starting test.");
    final CountDownLatch completeLatch = new CountDownLatch(threadCount);

    final List<ClientThread> clients = initDb(dbName, props, threadCount, targetPerThreadPerms,
        workload, tracer, completeLatch);

    if (status) {
      boolean standardStatus = false;
      if (props.getProperty(Measurements.MEASUREMENT_TYPE_PROPERTY, "").compareTo("timeseries") == 0) {
        standardStatus = true;
      }
      int statusIntervalSeconds = Integer.parseInt(props.getProperty("status.interval", "10"));
      boolean trackJVMStats = props.getProperty(Measurements.MEASUREMENT_TRACK_JVM_PROPERTY,
          Measurements.MEASUREMENT_TRACK_JVM_PROPERTY_DEFAULT).equals("true");
      statusthread = new StatusThread(completeLatch, clients, label, standardStatus, statusIntervalSeconds,
          trackJVMStats);
      statusthread.start();
    }

    Thread terminator = null;
    long st;
    long en;
    int opsDone;

    try (final TraceScope span = tracer.newScope(CLIENT_WORKLOAD_SPAN)) {

      final Map<Thread, ClientThread> threads = new HashMap<>(threadCount);
      for (ClientThread client : clients) {
        threads.put(new Thread(tracer.wrap(client, "ClientThread")), client);
      }

      st = System.currentTimeMillis();

      for (Thread t : threads.keySet()) {
        t.start();
      }

      if (maxExecutionTime > 0) {
        terminator = new TerminatorThread(maxExecutionTime, threads.keySet(), workload);
        terminator.start();
      }

      opsDone = 0;

      for (Map.Entry<Thread, ClientThread> entry : threads.entrySet()) {
        try {
          entry.getKey().join();
          opsDone += entry.getValue().getOpsDone();
        } catch (InterruptedException ignored) {
          // ignored
        }
      }

      en = System.currentTimeMillis();
    }

    try {
      try (final TraceScope span = tracer.newScope(CLIENT_CLEANUP_SPAN)) {

        if (terminator != null && !terminator.isInterrupted()) {
          terminator.interrupt();
        }

        if (status) {
          // wake up status thread if it's asleep
          statusthread.interrupt();
          // at this point we assume all the monitored threads are already gone as per above join loop.
          try {
            statusthread.join();
          } catch (InterruptedException ignored) {
            // ignored
          }
        }

        workload.cleanup();
      }
    } catch (WorkloadException e) {
      e.printStackTrace();
      e.printStackTrace(System.out);
      System.exit(0);
    }

    try {
      try (final TraceScope span = tracer.newScope(CLIENT_EXPORT_MEASUREMENTS_SPAN)) {
        exportMeasurements(props, opsDone, en - st);
      }
    } catch (IOException e) {
      System.err.println("Could not export measurements, error: " + e.getMessage());
      e.printStackTrace();
      System.exit(-1);
    }

    System.exit(0);
  }

  /**
   * DB to be tested, and init ClientThreads for this DB.
   * @param dbName
   * @param props
   * @param threadCount
   * @param targetRerThreadPerms
   * @param workload
   * @param tracer
   * @param completeLatch
   * @return
   */
  private static List<ClientThread> initDb(String dbName, Properties props, int threadCount,
                                           double targetRerThreadPerms, Workload workload, Tracer tracer,
                                           CountDownLatch completeLatch) {
    boolean initFailed = false;
    boolean doTransactions = Boolean.valueOf(props.getProperty(DO_TRANSACTIONS_PROPERTY, String.valueOf(true)));

    final List<ClientThread> clients = new ArrayList<>(threadCount);
    try (final TraceScope span = tracer.newScope(CLIENT_INIT_SPAN)) {

      int opCount;
      if (doTransactions) {
        opCount = Integer.parseInt(props.getProperty(OPERATION_COUNT_PROPERTY, "0"));
      } else {
        if (props.containsKey(INSERT_COUNT_PROPERTY)) {
          opCount = Integer.parseInt(props.getProperty(INSERT_COUNT_PROPERTY, "0"));
        } else {
          opCount = Integer.parseInt(props.getProperty(RECORD_COUNT_PROPERTY, DEFAULT_RECORD_COUNT));
        }
      }

      // init client threads, each have a DB instance
      for (int threadId=0; threadId<threadCount; threadId++) {
        DB db;
        try {
          db = DBFactory.newDB(dbName, props, tracer);
        } catch (UnknownDBException e) {
          System.out.println("Unknown DB " + dbName);
          initFailed = true;
          break;
        }

        int threadOpCount = opCount / threadCount;
        // ensure correct number of operations, in case opCount is not a multiple of threadCount
        if (threadId < opCount % threadCount) {
          ++threadOpCount;
        }

        ClientThread t = new ClientThread(
            db, doTransactions, workload, props, threadOpCount, targetRerThreadPerms, completeLatch);
        t.setThreadId(threadId);
        t.setThreadCount(threadCount);
        clients.add(t);
      }

      if (initFailed) {
        System.err.println("Error initializing dataStore bindings.");
        System.exit(0);
      }
    }
    return clients;
  }

  private static Tracer getTracer(Properties props, Workload workload) {
    return new Tracer.Builder("YCSB " + workload.getClass().getSimpleName())
        .conf(getHTraceConfiguration(props))
        .build();
  }

  private static void initWorkload(Properties props, Thread warningthread, Workload workload, Tracer tracer) {
    try {
      try (final TraceScope span = tracer.newScope(CLIENT_WORKLOAD_INIT_SPAN)) {
        workload.init(props);
        warningthread.interrupt();
      }
    } catch (WorkloadException e) {
      e.printStackTrace();
      e.printStackTrace(System.out);
      System.exit(0);
    }
  }

  private static HTraceConfiguration getHTraceConfiguration(Properties props) {
    final Map<String, String> filteredProperties = new HashMap<>();
    for (String key : props.stringPropertyNames()) {
      if (key.startsWith(HTRACE_KEY_PREFIX)) {
        filteredProperties.put(key.substring(HTRACE_KEY_PREFIX.length()), props.getProperty(key));
      }
    }
    return HTraceConfiguration.fromMap(filteredProperties);
  }

  /**
   * show a warning message that creating the workload is taking a while
   * but only do so if it is taking longer than 2 seconds
   * (showing the message right away if the setup wasn't taking very long was confusing people)
   * @return
   */
  private static Thread setupWarningThread() {
    return new Thread() {
      @Override
      public void run() {
        try {
          sleep(2000);
        } catch (InterruptedException e) {
          return;
        }
        System.err.println(" (might take a few minutes for large data sets)");
      }
    };
  }


  /**
   *
   * @param props
   * @return
   */
  private static Workload getWorkload(Properties props) {
    ClassLoader classLoader = Client.class.getClassLoader();

    try {
      Properties projectProp = new Properties();
      projectProp.load(classLoader.getResourceAsStream("project.properties"));
      System.out.println("YCSB Client " + projectProp.getProperty("version"));
    } catch (IOException e) {
      System.err.println("Unable to retrieve client version.");
    }

    System.out.println();
    System.out.println("Loading workload...");
    try {
      Class workloadClass = classLoader.loadClass(props.getProperty(WORKLOAD_PROPERTY));
      return (Workload) workloadClass.newInstance();
    } catch (Exception e) {
      e.printStackTrace();
      e.printStackTrace(System.out);
      System.exit(0);
    }

    return null;
  }

  private static Properties parseArguments(String[] args) {
    Properties props = new Properties();
    System.err.print("Command line:");
    for (String arg : args) {
      System.err.print(" " + arg);
    }

    Properties fileProps = new Properties();
    int argIndex = 0;

    if (args.length == 0) {
      usageMessage();
      System.out.println("At least one argument specifying a workload is required.");
      System.exit(0);
    }

    while (args[argIndex].startsWith("-")) {
      if (args[argIndex].compareTo("-threads") == 0) {
        argIndex++;
        if (argIndex >= args.length) {
          usageMessage();
          System.out.println("Missing argument value for -threads.");
          System.exit(0);
        }
        int tcount = Integer.parseInt(args[argIndex]);
        props.setProperty(THREAD_COUNT_PROPERTY, String.valueOf(tcount));
        argIndex++;
      } else if (args[argIndex].compareTo("-target") == 0) {
        argIndex++;
        if (argIndex >= args.length) {
          usageMessage();
          System.out.println("Missing argument value for -target.");
          System.exit(0);
        }
        int tTarget = Integer.parseInt(args[argIndex]);
        props.setProperty(TARGET_PROPERTY, String.valueOf(tTarget));
        argIndex++;
      } else if (args[argIndex].compareTo("-load") == 0) {
        props.setProperty(DO_TRANSACTIONS_PROPERTY, String.valueOf(false));
        argIndex++;
      } else if (args[argIndex].compareTo("-t") == 0) {
        props.setProperty(DO_TRANSACTIONS_PROPERTY, String.valueOf(true));
        argIndex++;
      } else if (args[argIndex].compareTo("-s") == 0) {
        props.setProperty(STATUS_PROPERTY, String.valueOf(true));
        argIndex++;
      } else if (args[argIndex].compareTo("-db") == 0) {
        argIndex++;
        if (argIndex >= args.length) {
          usageMessage();
          System.out.println("Missing argument value for -db.");
          System.exit(0);
        }
        props.setProperty(DB_PROPERTY, args[argIndex]);
        argIndex++;
      } else if (args[argIndex].compareTo("-l") == 0) {
        argIndex++;
        if (argIndex >= args.length) {
          usageMessage();
          System.out.println("Missing argument value for -l.");
          System.exit(0);
        }
        props.setProperty(LABEL_PROPERTY, args[argIndex]);
        argIndex++;
      } else if (args[argIndex].compareTo("-P") == 0) {
        argIndex++;
        if (argIndex >= args.length) {
          usageMessage();
          System.out.println("Missing argument value for -P.");
          System.exit(0);
        }
        String propfile = args[argIndex];
        argIndex++;

        Properties myfileProps = new Properties();
        try {
          myfileProps.load(new FileInputStream(propfile));
        } catch (IOException e) {
          System.out.println("Unable to open the properties file " + propfile);
          System.out.println(e.getMessage());
          System.exit(0);
        }

        //Issue #5 - remove call to stringPropertyNames to make compilable under Java 1.5
        for (Enumeration e = myfileProps.propertyNames(); e.hasMoreElements();) {
          String prop = (String) e.nextElement();

          fileProps.setProperty(prop, myfileProps.getProperty(prop));
        }

      } else if (args[argIndex].compareTo("-p") == 0) {
        argIndex++;
        if (argIndex >= args.length) {
          usageMessage();
          System.out.println("Missing argument value for -p");
          System.exit(0);
        }
        int eq = args[argIndex].indexOf('=');
        if (eq < 0) {
          usageMessage();
          System.out.println("Argument '-p' expected to be in key=value format (e.g., -p operationCount=99999)");
          System.exit(0);
        }

        String name = args[argIndex].substring(0, eq);
        String value = args[argIndex].substring(eq + 1);
        props.put(name, value);
        argIndex++;
      } else {
        usageMessage();
        System.out.println("Unknown option " + args[argIndex]);
        System.exit(0);
      }

      if (argIndex >= args.length) {
        break;
      }
    }

    if (argIndex != args.length) {
      usageMessage();
      if (argIndex < args.length) {
        System.out.println("An argument value without corresponding argument specifier (e.g., -p, -s) was found. "
            + "We expected an argument specifier and instead found " + args[argIndex]);
      } else {
        System.out.println("An argument specifier without corresponding value was found at the end of the supplied " +
            "command line arguments.");
      }
      System.exit(0);
    }

    //overwrite file properties with properties from the command line

    //Issue #5 - remove call to stringPropertyNames to make compilable under Java 1.5
    for (Enumeration e = props.propertyNames(); e.hasMoreElements();) {
      String prop = (String) e.nextElement();

      fileProps.setProperty(prop, props.getProperty(prop));
    }

    props = fileProps;

    if (!checkRequiredProperties(props)) {
      System.out.println("Failed check required properties.");
      System.exit(0);
    }

    return props;
  }
}
