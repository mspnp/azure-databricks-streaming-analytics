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
    con.withSessionDo(session => {
      val bound = session.prepare(
        s"""
           |insert into sqltest1.taxirecords1 (neighborhood,window_end,number_of_rides,total_fare_amount)
           |       values(?, ?, ?, ?)"""

      ).bind(
        record.getString(2),
        record.getTimestamp(1),
        record.getLong(3).asInstanceOf[AnyRef],
        record.getDouble(4).asInstanceOf[AnyRef]
      )

      session.execute(bound)
    })

  }

  def close(errorOrNull: Throwable): Unit = {
  }
}
