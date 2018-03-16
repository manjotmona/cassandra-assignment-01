package edu.knoldus

import scala.collection.JavaConverters._

import com.datastax.driver.core.{ConsistencyLevel, Session}
import edu.knoldus.DatabaseConnection.CassandraProvider

/**
 * Created by pallavi on 15/3/18.
 */

object CassandraCrud extends App with CassandraProvider {



  cassandraSession.getCluster.getConfiguration.getQueryOptions.setConsistencyLevel(ConsistencyLevel.QUORUM)
  // Connect to the cluster and keyspace "Assignment"
  println("\n\n*********Cluster Information *************")
  println("\n\n Cluster Name is: " + cassandraSession.getCluster.getClusterName)
  println("\n\n Cluster Configuration is: " +
          cassandraSession.getCluster.getConfiguration.getQueryOptions.getConsistencyLevel)
  println(
    "\n\n Cluster Metadata is: " + cassandraSession.getCluster.getMetadata.getAllHosts.toString)
  // Retrieve all User details from Users table

  println("\n\n*********Retrieve User Data Example *************")
  getUsersAllDetails(cassandraSession)

  // Insert new User into users table
  println("\n\n*********Insert User Data Example *************")
  cassandraSession.execute(
    "INSERT INTO employee (emp_id, emp_name, emp_city,emp_salary,emp_phone) VALUES (1, 'mona', " +
    "'Delhi',50000,97123846)")
  println("\n\n*********Inserted1 *************")
  cassandraSession.execute(
    "INSERT INTO employee (emp_id, emp_name, emp_city,emp_salary,emp_phone) VALUES (2, 'rahul', " +
    "'Delhi',40000,97123846)")
  println("\n\n*********Inserted2 *************")
  cassandraSession.execute(
    "INSERT INTO employee (emp_id, emp_name, emp_city,emp_salary,emp_phone) VALUES (3, 'mehak', " +
    "'Chandigarh',20000,97123846)")
  println("\n\n*********Inserted3 *************")
  cassandraSession.execute(
    "INSERT INTO employee (emp_id, emp_name, emp_city,emp_salary,emp_phone) VALUES (4, 'garima', " +
    "'Chandigarh',4000,97123846)")
  println("\n\n*********Inserted4 *************")
  cassandraSession.execute(
    "INSERT INTO employee (emp_id, emp_name, emp_city,emp_salary,emp_phone) VALUES (1, 'Name1', " +
    "'Delhi',50000,9876543210)")
  println("\n\n*********Inserted5 *************")


  // update record

  cassandraSession.execute(
  "update employee set emp_city = 'Chandigarh' where emp_id = 1 AND emp_salary = 50000")
  println("\n\n*************Updated************")
  getUsersAllDetails(cassandraSession)

  // 3rd question

  val results = cassandraSession.execute("SELECT * FROM employee where emp_id = 1 AND emp_salary > 30000").asScala.toList
  println("\n\n*************printing for question 3************")
  for (row <- results) {
    println("\n\n*************printing for question 3************")
    println("data: -" + row)
  }
  println("\n\n*************question3************")

  // 4rt

  cassandraSession.execute(
    "create index IF NOT EXISTS city on employee (emp_city)")
  println("\n\n*************Index created************")

  cassandraSession.execute(
    "select * FROM employee where emp_city = 'Chandigarh'")
  println("\n\n*************question 4 created************")


  getUsersAllDetails(cassandraSession)


  // 5th
  cassandraSession
    .execute(s"CREATE TABLE IF NOT EXISTS emp_by_city (emp_id int, emp_name text, emp_city text, " +
             s"emp_salary varint, emp_phone varint, PRIMARY KEY (emp_city,emp_id)) ")

  cassandraSession.execute(
    "INSERT INTO emp_by_city (emp_id, emp_name, emp_city,emp_salary,emp_phone) VALUES (1, 'Mona', " +
    "'Delhi',50000,97123846)")
  println("\n\n*********Inserted1 into emp_by_city *************")

  cassandraSession.execute(
    "INSERT INTO emp_by_city (emp_id, emp_name, emp_city,emp_salary,emp_phone) VALUES (2, 'Rahul', " +
    "'Chandigarh',50000,97123846)")
  println("\n\n*********Inserted2 into emp_by_city *************")
  cassandraSession.execute(
    "INSERT INTO emp_by_city (emp_id, emp_name, emp_city,emp_salary,emp_phone) VALUES (3, 'Jassy', " +
    "'Delhi',50000,97123846)")
  println("\n\n*********Inserted3 into emp_by_city *************")
  cassandraSession.execute(
    "INSERT INTO emp_by_city (emp_id, emp_name, emp_city,emp_salary,emp_phone) VALUES (4, 'Goldi', " +
    "'Chandigarh',50000,97123846)")
  println("\n\n*********Inserted4 into emp_by_city *************")

  val results1 = cassandraSession.execute("SELECT * FROM emp_by_city").asScala.toList
  for (row <- results1) {
    println("data: -" + row)
  }
  cassandraSession.execute(
  "delete from emp_by_city where emp_city = 'Chandigarh'")
  println("\n\n*********deleted into emp_by_city *************")
  val results2 = cassandraSession.execute("SELECT * FROM emp_by_city").asScala.toList
  for (row <- results2) {
    println("data: -" + row)
  }


  // Close Cluster and Session objects
  println("\n\nIs Cluster Closed :" + cassandraSession.isClosed)
  println("\n\nIs Session Closed :" + cassandraSession.isClosed)
  cassandraSession.close()
  println("\n\nIs Cluster Closed :" + cassandraSession.isClosed)
  println("\n\nIs Session Closed :" + cassandraSession.isClosed)

  private def getUsersAllDetails(inSession: Session): Unit = {

    inSession
      .execute(s"CREATE TABLE IF NOT EXISTS employee (emp_id int, emp_name text, emp_city text, " +
               s"emp_salary varint, emp_phone varint, PRIMARY KEY (emp_id, emp_salary)) ")

    println("******************table created***************")
    // Use select to get the users table data
    val results = inSession.execute("SELECT * FROM employee").asScala.toList
    for (row <- results) {
      println("data: -" + row)
    }
  }
}
