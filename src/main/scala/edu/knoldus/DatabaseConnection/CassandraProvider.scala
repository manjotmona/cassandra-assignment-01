package edu.knoldus.DatabaseConnection

import com.datastax.driver.core.{Cluster, Session}
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

/**
 * Created by pallavi on 14/3/18.
 */

trait CassandraProvider {
  val logger = LoggerFactory.getLogger(getClass.getName)
  val config = ConfigFactory.load()
  val cassandraKeyspace = config.getString("cassandra.keyspace")
  val cassandraHostname = config.getString("cassandra.contact.points")

  val cassandraSession: Session = {
    val cluster = new Cluster.Builder().withClusterName("Test Cluster").
      addContactPoints(cassandraHostname).build
    val session = cluster.connect
    session.execute(s"CREATE KEYSPACE IF NOT EXISTS  ${cassandraKeyspace} WITH REPLICATION = " +
                    s"{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }")
    session.execute(s"USE ${cassandraKeyspace}")
    println("***************keyspace created****************")
    // Optional method that can be implemented if table creation scripts are required
    //createTables(session)
    session
  }
}