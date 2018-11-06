package com.microsoft.pnp

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.{ForeachWriter, Row}

class CassandraSinkForeach(con: CassandraConnector)
  extends ForeachWriter[Row] {

  // This class implements the interface ForeachWriter, which has methods that get called
  // whenever there is a sequence of rows generated as output
  def open(partitionId: Long, version: Long): Boolean = {
    true
  }

  def process(record: Row) = {
    println(s"Process new $record")
    con.withSessionDo(session => {
      val ps = session.prepare(
        s"""
           |insert into sqltest1.taxirecords1 (neighborhood,window_end,number_of_rides,total_fare_amount)
           |       values( :n, :w, :r, :f)"""

      )

      val bound = ps.bind()
      bound
        .setString("n", record.getString(2))
        .setTimestamp("w", record.getTimestamp(1))
        .setLong("r", record.getLong(3))
        .setDouble("f", record.getDouble(4))

      session.execute(bound)
    })

  }

  def close(errorOrNull: Throwable): Unit = {
  }
}
