
package db_test

import com.datastax.driver.core._
import collection.JavaConversions._
import java.io.PrintWriter
import hist.Histogram
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.commons.cli._
import java.util.concurrent.{TimeUnit, Executors}
import collection.mutable

/**
 * Benchmarking class for Cassandra.
 *
 * @param address The cluster address (IP).
 * @param limit The number of lines to insert into the database.
 */
class CassandraTest(val address: String, val limit: Int, val nThreads: Int) {

  private val cluster = Cluster.builder.addContactPoint(address).build        // Cluster identifier
  private val session = cluster.connect()                                     // Session identifier

  //---------------------------------------------------------------------------------

  /** Shutdown the cluster */
  def shutdown(): Unit =
    cluster.close()

  /** Printout information regarding the test and the cluster **/
  def info(): Unit = {
    println("  - Doing a simple test for %,d values using %,d threads." format (limit, nThreads))

    val metadata = cluster.getMetadata
    println("  - Connected to cluster '%s'." format metadata.getClusterName)
  }

  /** Create database (or drops it and recreates it from scratch */
  def createSchema(): Unit = {
    try {
      session.execute("DROP KEYSPACE cassandra_test;")
    }
    catch {
      case e : Exception => println("  - Keyspace 'cassandra_test' didn't exist, excellent! Carrying on. (%s)" format e.getMessage)
    }

    session.execute("CREATE KEYSPACE cassandra_test WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};")

    println("  - Created keyspace 'cassandra_test'.")

    session.execute("CREATE TABLE cassandra_test.mytable (" +
      "id varchar primary key," + Array.range(1,25).map("m%02d double" format _).mkString(",") + ");")

    println("  - Created table 'mytable'.")
  }

  /**
   * Create 'nConnections' to perform operations on Cassandra.
   *
   * @param nConnections Number of requested connections.
   * @return A sequence with the connections created.
   */
  private def createConnections(nConnections: Int): Seq[Session] =
    Range(0, nConnections).map(_ => cluster.connect())

  /**
   * Close all the connections to a database.
   *
   * @param connections The connections to close.
   */
  private def destroyConnections(connections: Seq[Session]): Unit =
    connections.foreach { _.close() }

  /**
   * Create prepared statements for inserting data into the database.
   *
   * @param connections The connections to be used. The results will be a prepared statement per connection.
   * @return A stack containging Tuple2s of (Session, PreparedStatement)
   */
  private def createStatementsForInsert(connections: Seq[Session]): mutable.Stack[Tuple2[Session, PreparedStatement]] = {
    val statements = new scala.collection.mutable.Stack[Tuple2[Session, PreparedStatement]]

    for (connection <- connections) {
      val insertStm: PreparedStatement = connection.prepare(
        "INSERT INTO cassandra_test.mytable(id, m01, m02, m03, m04, m05, m06, m07, m08, m09, m10, m11, m12, m13, m14, m15, m16, m17, m18, m19, m20, m21, m22, m23, m24)" +
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);")

      statements push ((connection, insertStm))
    }

    statements
  }

  /**
   * Create prepared statements for reading data from the database.
   *
   * @param connections The connections to be used. The results will be a prepared statement per connection.
   * @return A stack containing Tuple2s of (Session, PreparedStatement)
   */
  private def createStatementsForQuerying(connections: Seq[Session]): mutable.Stack[Tuple2[Session, PreparedStatement]] = {
    val statements = new scala.collection.mutable.Stack[Tuple2[Session, PreparedStatement]]

    for (connection <- connections) {
      val selectStm: PreparedStatement = connection.prepare("SELECT * FROM cassandra_test.mytable WHERE id = ?;")

      statements push ((connection, selectStm))
    }

    statements
  }

    /**
     * Load data into the cluster performing a "write test".
     *
     * The test consists in inserting a certain number of lines where each line consists in an identifier and 24 measures (doubles).
     * All times are recorded so that detailed statistics can be calculated (return of the function).
     *
     * @return Array with all the time samples it took to insert the data.
     */
  def writeTest(): Array[Long] = {
    val connections = createConnections(nThreads)
    val insertStms = createStatementsForInsert(connections)

    // Results will be collected from all threads that will execute the test without no processing until we're done
    val samples = new Array[Long](limit)
    val t1 = System.nanoTime

    // For load balancing proposes, each chuck of work is about 20% of the whole amount divided by the number of threads
    val threadPool = Executors.newFixedThreadPool(nThreads)
    val slice = limit/nThreads/5

    Range(0, limit, slice) foreach { startIndex =>
      threadPool execute WriteWorkSlice(startIndex, slice, limit, samples, insertStms)
    }
    threadPool.shutdown()
    threadPool.awaitTermination(Long.MaxValue, TimeUnit.NANOSECONDS)

    // Test is completed at this point

    val t2 = System.nanoTime

    destroyConnections(connections)

    val totalEffectivelyInserted = samples.count(_ != 0)
    val throughput = 1.0e9 * totalEffectivelyInserted / (t2 - t1 + 1)

    val realTime = samples.sum + 1
    val realThroughput = 1.0e9 * totalEffectivelyInserted * nThreads / realTime

    println("  - Done inserting data. Total Time = %,.1f sec. Total elements = %,d. Wall-time Throughput = %,.0f elem/sec. Real Throughput = %,.0f elem/sec" format ((t2-t1)/1.0e9, totalEffectivelyInserted, throughput, realThroughput))

    // Return the samples without the zero elements (which represent errors)
    samples.filter(_ != 0)
  }

  /**
   * Performs a "read test".
   *
   * The test consists in reading a certain number of lines where each line consists in an identifier and 24 measures (doubles).
   * All times are recorded so that detailed statistics can be calculated (return of the function).
   *
   * @return Array with all the time samples it took to insert the data.
   */
  def readTest(): Array[Long] = {
    val connections = createConnections(nThreads)
    val readStms = createStatementsForQuerying(connections)

    // Results will be collected from all threads that will execute the test without no processing until we're done
    val samples = new Array[Long](limit)
    val t1 = System.nanoTime

    // For load balancing proposes, each chuck of work is about 20% of the whole amount divided by the number of threads
    val threadPool = Executors.newFixedThreadPool(nThreads)
    val slice = limit/nThreads/5

    Range(0, limit, slice) foreach { startIndex =>
      threadPool execute ReadWorkSlice(startIndex, slice, limit, samples, readStms)
    }
    threadPool.shutdown()
    threadPool.awaitTermination(Long.MaxValue, TimeUnit.NANOSECONDS)

    // Test is completed at this point

    val t2 = System.nanoTime

    destroyConnections(connections)

    val totalEffectivelyRead = samples.count(_ != 0)
    val throughput = 1.0e9 * totalEffectivelyRead / (t2 - t1 + 1)

    val realTime = samples.sum + 1
    val realThroughput = 1.0e9 * totalEffectivelyRead * nThreads / realTime

    println("  - Done querying data. Total Time = %,.1f sec. Total elements = %,d. Wall-time Throughput = %,.0f elem/sec. Real Throughput = %,.0f elem/sec" format ((t2-t1)/1.0e9, totalEffectivelyRead, throughput, realThroughput))

    // Return the samples without the zero elements (which represent errors)
    samples.filter(_ != 0)
  }

  /**
   * Saves the results to file and displays an histogram of the results.
   *
   * @param samples An array containing all the times (in nanosecs) for the performed operations
   * @param fileSufix Suffix to use in the output file
   * @param dumpToDisk Writes data to disk.
   */
  def stats(samples: Array[Long], fileSufix: String, dumpToDisk: Boolean): Unit = {
    // Write all data into file

    if (dumpToDisk) {
      val fileName = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss").format(new Date) + ("__%s.csv" format fileSufix)
      println("  - Writing the results into '%s' (%,d lines)" format (fileName, limit))

      val pw = new PrintWriter(fileName)
      samples.foreach { pw.println }
      pw.close()
    }

    // Show an histogram of the results
    val bins = List[Double](0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 10.0, 20.0)
    val histogram = new Histogram(samples.map { _/100000 / 10.0 }.toList, bins)

    println("  - Here's a histogram of the latencies (in msec):")
    histogram.print()
  }

  /** Query the database for some ids */
  def simpleQuery(): Unit = {
    print("  - Getting a few ids: ")

    val result = session.execute("SELECT id FROM cassandra_test.mytable LIMIT 3;")
    val ids = "[ %s ]" format result.map(_.getString(0)).mkString(" | ")
    println(ids)
  }
}

/** Main test driver */
object CassandraTest  {
  private val defaultTestSize = 10000
  private val defaultThreads = 1
  private val defaultIP = "127.0.0.1"

  case class TestOptions(
    limit: Int = defaultTestSize,
    nThreads: Int = defaultThreads,
    writeTest: Boolean = true,
    readTest: Boolean = true,
    dump: Boolean = false,
    ip: String = defaultIP) {
  }

  //----------------------------------------------------------------------------------------------

  def parseCommandLine(args: Array[String]): TestOptions = {
    val prgOptions = new Options
    prgOptions.addOption("help", false, "help, prints this message")
    prgOptions.addOption("read", false, "read test")
    prgOptions.addOption("write", false, "write test")
    prgOptions.addOption("dump", false, "dump test results to disk")
    prgOptions.addOption("size", true, "size of the test (default=%,d)" format defaultTestSize)
    prgOptions.addOption("threads", true, "number of threads to used (default=%,d)" format defaultThreads)
    prgOptions.addOption("ip", true, "ip address where cassandra is (default=%s)" format defaultIP)

    val cmdOptions: CommandLine = try {
      (new GnuParser).parse(prgOptions, args)
    }
    catch {
      case optionException: UnrecognizedOptionException => {
        System.err.println(optionException.getMessage)
        (new HelpFormatter).printHelp("CassandraTest", prgOptions, true)
        sys.exit(0)
      }
    }

    if (cmdOptions.getArgs.length != 0) {
      System.err.println("Invalid option specified.")
      (new HelpFormatter).printHelp("CassandraTest", prgOptions, true)
      sys.exit(0)
    }

    if (cmdOptions hasOption "help") {
      (new HelpFormatter).printHelp("CassandraTest", prgOptions, true)
      sys.exit(0)
    }

    // Parse all the options into an object by using foldLeft and copying the options one by one
    val options = (TestOptions() /: cmdOptions.getOptions) { (tstOptions: TestOptions, current: Option) =>
      current.getOpt match {
        case "read"     => tstOptions.copy(readTest = true)
        case "write"    => tstOptions.copy(writeTest = true)
        case "dump"     => tstOptions.copy(dump = true)
        case "size"     => tstOptions.copy(limit = current.getValue.toInt)
        case "threads"  => tstOptions.copy(nThreads = current.getValue.toInt)
        case "ip"       => tstOptions.copy(ip = current.getValue)
      }
    }

    options
  }

  //----------------------------------------------------------------------------------------------

  def main(args: Array[String]) {
    val testOptions = parseCommandLine(args)

    val test = new CassandraTest(testOptions.ip, testOptions.limit, testOptions.nThreads)
    test.info()

    if (testOptions.writeTest) {
      test.createSchema()
      val samples = test.writeTest()
      test.simpleQuery()
      test.stats(samples, "wr", testOptions.dump)
    }

    if (testOptions.readTest) {
      val samples = test.readTest()
      test.stats(samples, "rd", testOptions.dump)
    }

    test.shutdown()
  }
}
