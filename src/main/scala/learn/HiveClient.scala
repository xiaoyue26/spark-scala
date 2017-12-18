package learn

import java.sql.DriverManager
import java.util

/**
  * Created by xiaoyue26 on 17/9/2.
  */
object HiveClient {
  def execute(sql: String): util.List[util.List[AnyRef]] = {
    val url = "jdbc:hive2://f04:10000/default"
    val rows = new util.ArrayList[util.List[AnyRef]]
    try { //Class.forName(driver);
      var conn = DriverManager.getConnection(url, "pipe", "")
      val statement = conn.createStatement
      val flag = statement.execute(sql)

      if (false == flag) {
        return rows
      }
      val resultSet = statement.getResultSet
      val resultMeta = resultSet.getMetaData
      val num = resultMeta.getColumnCount
      while (resultSet.next) {
        val row = new util.ArrayList[AnyRef]
        var i = 1
        while (i < num + 1) {
          row.add(resultSet.getObject(i))
          i += 1
        }
        rows.add(row)
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    return rows
  }
  def executeBatch(sql: String): util.List[util.List[AnyRef]] = {
    val url = "jdbc:hive2://f04:10000/default"
    val rows = new util.ArrayList[util.List[AnyRef]]
    try { //Class.forName(driver);
      var conn = DriverManager.getConnection(url, "pipe", "")
      val statement = conn.createStatement

      statement.addBatch(sql)
      statement.executeBatch()
      val flag = statement.execute(sql)

      if (false == flag) {
        return rows
      }
      val resultSet = statement.getResultSet
      val resultMeta = resultSet.getMetaData
      val num = resultMeta.getColumnCount
      while (resultSet.next) {
        val row = new util.ArrayList[AnyRef]
        var i = 1
        while (i < num + 1) {
          row.add(resultSet.getObject(i))
          i += 1
        }
        rows.add(row)
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    return rows
  }

  def main(args: Array[String]): Unit = {

  }
}
