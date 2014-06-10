package org.nicta.hdm.ml

import org.junit.Test
import org.nicta.unsw.ml.DataParser

/**
 * Created by Dongyao.Wu on 2014/5/28.
 */
class DataParserTest {

  @Test
  def testToDouble(){
    println(DataParser.toDouble("+23"))
    println(DataParser.toDouble("-23"))
    println(DataParser.toDouble("-0.23"))
    println(DataParser.toDouble("^&*-0.23"))
  }

  @Test
  def testLoadSingleLine(){
    val path = "C:\\study\\machinelearning\\unsw-assignments\\assignment-2\\data\\hospital_data\\Healthcare_Associated_Infections.csv"
    val data = DataParser.parseCsv(path, """,""", 0, 11)
    println(data.size)
    data.take(100).foreach(println(_))
  }

  @Test
  def testLoadMultipleLines(){
    val path = "C:\\study\\machinelearning\\unsw-assignments\\assignment-2\\data\\hospital_data\\HOSPITAL_QUARTERLY_QUALITYMEASURE_IPFQR_HOSPITAL.csv"
    val indexes = Seq(9,10)
    val data = DataParser.parseCsv(path, """,""", 0, indexes)
    println(data.size)
    data.take(100).foreach(println(_))
  }

  @Test
  def testLoadMultipleFiles(){

    val pathMap = Map(
      "C:\\study\\machinelearning\\unsw-assignments\\assignment-2\\data\\hospital_data\\Healthcare_Associated_Infections.csv" -> Seq(11),
      "C:\\study\\machinelearning\\unsw-assignments\\assignment-2\\data\\hospital_data\\HOSPITAL_QUARTERLY_QUALITYMEASURE_IPFQR_HOSPITAL.csv" ->Seq(9,10),
      "C:\\study\\machinelearning\\unsw-assignments\\assignment-2\\data\\hospital_data\\Immunization.csv" -> Seq(12),
      "C:\\study\\machinelearning\\unsw-assignments\\assignment-2\\data\\hospital_data\\Medicare hospital spending per patient.csv" -> Seq(11),
      "C:\\study\\machinelearning\\unsw-assignments\\assignment-2\\data\\hospital_data\\Medicare Volume Measures.csv" -> Seq(11),
      "C:\\study\\machinelearning\\unsw-assignments\\assignment-2\\data\\hospital_data\\Outcome of Care Measures.csv" -> (11 to 64).toList,
      "C:\\study\\machinelearning\\unsw-assignments\\assignment-2\\data\\hospital_data\\Outpatient Imaging Efficiency Measures.csv" -> (10 to 26).toList
    )
    val dataList = pathMap.map{ tuple =>
//      println(tuple._1)
//      println(tuple._2)
      val data = DataParser.parseCsv(tuple._1, """,""", 0, tuple._2)
//      println(data.size)
//      data.take(10).foreach(println(_))
      tuple._1 -> data
    }
    dataList.foreach(d => println(d._1 + d._2.size))
//    val grouppData = dataList.toSeq.map
//    grouppData.take(10).foreach(println(_))
//    println(grouppData.size)
  }

  @Test
  def seqTest(){
    val path = "C:\\study\\machinelearning\\unsw-assignments\\assignment-2\\data\\hospital_data\\Outcome of Care Measures.csv"
    val indexes = 11 to 64
    val data = DataParser.parseCsv(path, """,""", 0, indexes)
    println(data.size)
    data.take(100).foreach(println(_))
  }


}
