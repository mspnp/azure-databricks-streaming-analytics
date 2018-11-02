package com.microsoft.pnp

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.{ForeachWriter, Row}

class CassandraSinkForeach(con: CassandraConnector, keySpace: String, tableName: String)
  extends ForeachWriter[Row] {

  // This class implements the interface ForeachWriter, which has methods that get called
  // whenever there is a sequence of rows generated as output
  def open(partitionId: Long, version: Long): Boolean = {
    true
  }

  def process(record: Row) = {
    println(s"Process new $record")
    con.withSessionDo(session =>
      session.execute(
        s"""
           |insert into $keySpace.$tableName (neighborhood,window_end,number_of_rides,total_fare_amount)
           |       values('${record(2)}','${record(1)}',${record(3)},${record(4)})"""

      )
    )
  }

  def close(errorOrNull: Throwable): Unit = {
  }
}
