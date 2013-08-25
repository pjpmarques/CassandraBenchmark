package db_test

import scala.collection.mutable.Stack
import com.datastax.driver.core.{PreparedStatement, Session}
import collection.mutable

/**
 * Represents a piece of work to be performed by a thread.
 */
abstract class WorkSlice extends Runnable

/**
 * Class that represents a piece of work to be done by a thread when inserting into Cassandra.
 * When doing the test a number of inserts ('limit') is going to be put into cassandra. This piece of work
 * represents the inserts from 'start' to 'start+limit'. In the case of the last work slice, inserts will
 * not be done past 'limit'.
 *
 * 'samples' represent an array that needs to be updated with the number of nanoseconds it took to do each insert.
 * 'dbStms' represent a pair of session/prepared statement to be used to insert into cassandra. The thread needs
 * to take on of these out and but it back in at the end.
 *
 * @param start The start index of the data to insert into cassandra.
 * @param slice How much data to insert into cassandra
 * @param limit The upper limit for the data to insert (i.e., the top bound of the buffer)
 * @param samples Array to where the time samples of the inserts must be stored
 * @param dbStms Pair[Session, PreparedStatement] from where to use cassandra
 */
case class WriteWorkSlice(start: Int, slice: Int, limit: Int, samples: Array[Long], dbStms: mutable.Stack[Pair[Session, PreparedStatement]]) extends WorkSlice  {
  def run(): Unit = {
    // Get a session and prepared statement to use with cassandra
    val (thisSession, insertStm) = dbStms.pop()

    // Make sure we don' write past the end of the array
    val end = if (start+slice > limit) limit else start+slice
    val random = util.Random
    var totalInserted = 0

    // Start test
    val t1 = System.nanoTime

    // If an error occurs we need to restore the connection that was used
    try {
      for (i <- start until end) {
        val id = ("%020d" format i).reverse
        val m = Array.fill(24) { random.nextDouble().asInstanceOf[java.lang.Double] }

        val tA = System.nanoTime
        val bound = insertStm bind (id, m(0), m(1), m(2), m(3), m(4), m(5), m(6), m(7), m(8), m(9), m(10), m(11), m(12), m(13), m(14), m(15), m(16), m(17), m(18), m(19), m(20), m(21), m(22), m(23))
        thisSession.execute(bound)
        val tB = System.nanoTime

        samples(i) = (tB-tA).toInt
        totalInserted+= 1
      }
    }
    catch {
      case e: Throwable => println("  - A problem occurred while inserting data item %,d out of %,d.\n    Cause: %s" format (totalInserted+1, end-start, e.getMessage))
    }
    finally {
      dbStms push ((thisSession, insertStm))
    }

    val t2 = System.nanoTime
    val elapsed = t2-t1
    val throughput = 1.0e9 * totalInserted / elapsed

    println("  - Inserted %,7d elements in %,6.1f sec (%,.0f elem/thread/sec)" format (totalInserted, elapsed/1.0e9, throughput))
  }
}

/**
 * Class that represents a piece of work to be done by a thread when reading data from Cassandra.
 *
 * 'samples' represent an array that needs to be updated with the number of nanoseconds it took to do each insert.
 * 'dbStms' represent a pair of session/prepared statement to be used to read data from cassandra. The thread needs
 * to take on of these out and but it back in at the end.
 *
 * @param slice How much data to read from Cassandra.
 * @param limit The upper limit for the identifiers that were inserted.
 * @param samples Array to where the time samples of the inserts must be stored
 * @param dbStms Pair[Session, PreparedStatement] from where to use cassandra
 */
case class ReadWorkSlice(start: Int, slice: Int, limit: Int, samples: Array[Long], dbStms: mutable.Stack[Pair[Session, PreparedStatement]]) extends WorkSlice {
  def run(): Unit = {
    // Get a session and prepared statement to use with cassandra
    val (thisSession, readStm) = dbStms.pop()

    // Make sure we don' write past the end of the array
    val end = if (start+slice > limit) limit else start+slice
    val random = util.Random
    var totalRead = 0

    // Start test
    val t1 = System.nanoTime

    // If an error occurs we need to restore the connection that was used
    try {
      for (i <- start until end) {
        val id = ("%020d" format random.nextInt(limit))

        val tA = System.nanoTime
        val bound = readStm bind id
        thisSession.execute(bound)
        val tB = System.nanoTime

        samples(i) = (tB-tA).toInt
        totalRead+= 1
      }
    }
    catch {
      case e: Throwable => println("  - A problem occurred while reading data item %,d out of %,d.\n    Cause: %s" format (totalRead+1, slice, e.getMessage))
    }
    finally {
      dbStms push ((thisSession, readStm))
    }

    val t2 = System.nanoTime
    val elapsed = t2-t1
    val throughput = 1.0e9 * totalRead / elapsed

    println("  - Read %,7d elements in %,6.1f sec (%,.0f elem/thread/sec)" format (totalRead, elapsed/1.0e9, throughput))
  }
}

