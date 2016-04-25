package org.nicta.hdm.functions

import org.junit.Test
import org.nicta.wdy.hdm.Buf
import org.nicta.wdy.hdm.functions._

/**
 * Created by Tiantian on 2014/12/16.
 */
class ParallelFunctionsTest {


  val text =
    """
        this is a word count text
        this is line 2
        this is line 3
    """.split("\\s+").toIterator

  val numberArray = Seq.fill(10){Math.random()}

  @Test
  def testMapFunc(){
    new ParMapFunc[String, Int]((d) => d match {
      case s:String  => s.length
      case _ => 0
    }).apply(text).foreach(println(_))
  }

  @Test
  def textMapAggregation(): Unit ={
    val res = Buf.empty[Int]
    val map = new ParMapFunc[String, Int]((d) => d match {
      case s:String  => s.length
      case _ => 0
    })
    for (i <- 1 to 10)
      map.aggregate(text,res)
    res.foreach(println(_))
  }

  @Test
  def testMultiMap(): Unit ={

  }

  @Test
  def testGroupByFunc(){
    val f = (d:String) => d match {
      case s if(s.length >= 3)=> s.substring(0,3)
      case ss => ss
    }

    //    ddm.elems.groupBy(f).foreach(println(_))
    val res = new ParGroupByFunc[String,String](f).apply(text)
    res.foreach(println(_))
  }

  @Test
  def testGroupByAggregation(): Unit ={
    val f = (d:String) => d match {
      case s if(s.length >= 3)=> s.substring(0,3)
      case ss => ss
    }
    val gb = new ParGroupByFunc[String,String](f)
    var res: Buf[(String,Iterable[String])] = Buf.empty[(String, Iterable[String])]
    for (i <- 1 to 10)
      res = gb.aggregate(text,res)
    res.foreach(println(_))
  }

  @Test
  def testReduceFunc(){
    val f = (d:String) => d match {
      case s if(s.length >= 3)=> s.substring(0,3)
      case ss => ss
    }

    val grouped = new ParGroupByFunc[String,String](f).apply(text)
    val res = new ParReduceFunc[(String, Iterable[String]), (String, Iterable[String])]((s1,s2) => (s2._1, s1._2 ++ s2._2)).apply(grouped)
    res.foreach(println(_))
  }

  @Test
  def testReduceAggregation(){
    val f = (d:String) => d match {
      case s if(s.length >= 3)=> s.substring(0,3)
      case ss => ss
    }

    val grouped = new ParGroupByFunc[String,String](f).apply(text)
    val reduce = new ParReduceFunc[(String, Iterable[String]), (String, Iterable[String])]((s1,s2) => (s2._1, s1._2 ++ s2._2))
    var res:Buf[(String,Iterable[String])] = Buf.empty[(String,Iterable[String])]
    for (i <- 1 to 3)
      res = reduce.aggregate(grouped,res)
    res.foreach(println(_))
  }


  @Test
  def testReduceByKeyFunc(){
    val mapped = new ParMapFunc[String,(String,Int)]((_, 1)).apply(text)
    val res = new ParReduceBy[(String,Int), String](_._1, (s1,s2) => (s1._1, s1._2 + s2._2)).apply(mapped)
    res.foreach(println(_))
  }

  @Test
  def testReduceByKeyAggregation(){
    val mapped = new ParMapFunc[String,(String,Int)]((_, 1)).apply(text)
    val reduceByK = new ParReduceBy[(String,Int), String](_._1, (s1,s2) => (s1._1, s1._2 + s2._2))
    var res:Buf[(String,(String,Int))] = Buf.empty[(String,(String,Int))]
    for (i <- 1 to 3)
      res = reduceByK.aggregate(mapped,res)
    res.foreach(println(_))
  }


  @Test
  def testGroupFoldByKey(){

    val mapped = new ParMapFunc[String,(String,Int)]((_, 1)).apply(text)
    val res = new ParGroupByAggregation[(String,Int),String, Int](_._1, _._2, _+_).apply(mapped)
    res.foreach(println(_))

  }

  @Test
  def testFunctionComposition(): Unit ={
    val f =  (w:String) => {
      val as = w.split(",")
      (as(0), 1)
    }

    val mapF = new ParMapFunc[String,(String,Int)](f)
    val mapped = mapF.apply(text)
    val groupBy = new ParGroupByFunc[(String,Int),String](_._1)
    val mv = (t:(String, Iterable[(String,Int)])) => {
      (t._1, t._2.map(_._2).reduce(_+_))
    }
    val mapValues = new ParMapFunc(mv)
//    val groupByKeyF = new ParGroupFoldByKey[(String,Int),String, Int](_._1, _._2, _+_)
    println("====== test AndThen =====")
    val andThenF = groupBy.andThen(mapValues)
    andThenF.apply(mapped).foreach(println(_))
    println("====== test Composition =====")
    mapValues.compose(groupBy).apply(mapped).foreach(println(_))
  }

  @Test
  def testFunctionCompositionAggregation(): Unit ={
    val f =  (w:String) => {
      val as = w.split(",")
      (as(0), 1)
    }

    val mapF = new ParMapFunc[String,(String,Int)](f)
    val mapped = mapF.apply(text)

    val groupBy = new ParGroupByFunc[(String,Int),String](_._1)
    val mv = (t:(String, Iterable[(String,Int)])) => {
      (t._1, t._2.map(_._2).reduce(_+_))
    }
    val mapValues = new ParMapFunc(mv)
//    val groupByKeyF = new ParGroupFoldByKey[(String,Int),String, Int](_._1, _._2, _+_)
    println("====== test AndThen =====")
    val andThenF = groupBy.andThen(mapValues).asInstanceOf[ParCombinedFunc[(String,Int), _ ,(String,Int)]]
    val andThenF2 = andThenF.asInstanceOf[ParCombinedFunc[(String,Int), andThenF.mediateType.type ,(String,Int)]]
    var res:Buf[(String,Int)] = Buf.empty[(String,Int)]
    var partialRes:Buf[andThenF.mediateType.type ] = Buf.empty[andThenF.mediateType.type ]
    for (i <- 1 to 3)
      partialRes = andThenF2.partialAggregate(mapped,partialRes)
    res = andThenF2.postF(partialRes.toIterator).toBuffer
    res.foreach(println(_))
    println("====== test Composition =====")
    val combined = mapValues.compose(groupBy)
    res = null
    for (i <- 1 to 3)
      res = combined.aggregate(mapped,res)
    res.foreach(println(_))
  }
}
