package learn

import java.sql.{Connection, DriverManager, ResultSet, ResultSetMetaData}

/**
  * Created by xiaoyue26 on 17/7/21.
  */
class MysqlClient(var host: String, var db: String, var user: String, var password: String) {
  def query(sql: String): List[List[Object]] = {
    MysqlClient.query(sql, host, db, user, password)
  }
}

object MysqlClient {
  val driver = "com.mysql.jdbc.Driver"

  def getUrl(host: String, db: String, user: String, password: String): String =
    "jdbc:mysql://%s:3306/%s?user=%s&password=%s".format(host, db, user, password)

  def query(sql: String, host: String, db: String, user: String, password: String): List[List[Object]] = {
    val url = getUrl(host, db, user, password)
    var conn: Connection = null
    var rows: List[List[Object]] = List()
    try {
      Class.forName(driver)
      conn = DriverManager.getConnection(url)
      val statement = conn.createStatement()
      val resultSet: ResultSet = statement.executeQuery(sql)
      val resultMeta: ResultSetMetaData = resultSet.getMetaData
      val num = resultMeta.getColumnCount
      while (resultSet.next()) {
        var row: List[Object] = List()
        for (i <- 1 to num) {
          row = row :+ resultSet.getObject(i)
        }
        rows = rows :+ row
      }
    } catch {
      case e: java.sql.SQLException => println(e.getMessage)
      case e: Throwable => e.printStackTrace()
    }
    finally {
      conn.close()
    }
    rows
  }

  def main(args: Array[String]): Unit = {
    // test object
    val sql = "select * from test"
    //val sql = "replace into test(name,col1)values('test_name','1')"
    val host = "localhost"
    val db = "pipe_ape"
    val user = "root"
    val password = "mysql"
    val rows = query(sql, host, db, user, password)
    rows.foreach(
      row => row.foreach {
        case null =>
        case col => println(col.getClass)
      }
    )
    println(rows)
    // test instance
    val mysqlInstance = new MysqlClient(host, db, user, password)
    mysqlInstance.db = "test"
    mysqlInstance.query(sql) foreach println
  }

}

object LocalMysqlClient {
  def query(sql: String): List[List[Object]] = {
    val host = "localhost"
    val db = "pipe_ape"
    val user = "root"
    val password = "mysql"
    MysqlClient.query(sql, host, db, user, password)
  }

  def main(args: Array[String]): Unit = {
    val sql = "select * from test"
    query(sql) foreach println
  }
}

object PipeMysqlClient {
  def query(sql: String): List[List[Object]] = {
    val host = "pipe-writer"
    val db = "pipe_ape"
    val user = "pipe"
    val password = "pipe123"
    MysqlClient.query(sql, host, db, user, password)
  }

  def main(args: Array[String]): Unit = {
    val sql = "select * from test"
    query(sql) foreach println
  }
}