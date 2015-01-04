package org.nicta.hdm.executor

import com.baidu.bpit.akka.server.SmsSystem
import org.junit.{After, Test, Before}
import org.nicta.wdy.hdm.executor.HDMContext
import org.nicta.wdy.hdm.functions.ParUnionFunc
import org.nicta.wdy.hdm.io.Path
import org.nicta.wdy.hdm.model.{HDM, DFM, DDM}

import scala.concurrent.ExecutionContext
import scala.reflect.{classTag,ClassTag}
import scala.util.{Failure, Success}
import ExecutionContext.Implicits.global

/**
 * Created by tiantian on 3/01/15.
 */
class HDMClusterTest extends ClusterTestSuite{

  val text =
    """
        this is a word count text
        this is line 2
        this is line 3
    """.split("\\s+")

  val text2 =
    """
        this is a word count text
        this is line 4
        this is line 5
        this is line 6
        this is line 7
    """.split("\\s+")


  @Before
  def beforeTest(): Unit ={
    HDMContext.init()
  }

  @Test
  def testStartFollower(): Unit ={
    new Thread {
      override def run {
        HDMContext.init(leader ="akka.tcp://masterSys@127.0.0.1:8999/user/smsMaster")
      }
    }.start
    Thread.sleep(60000)
  }

  @Test
  def testSendSerializableMsg(): Unit ={
    new Thread {
      override def run {
        HDMContext.init(leader ="akka.tcp://masterSys@127.0.0.1:8999/user/smsMaster")
        val hdm = new DFM[String,String](children = null, func = new ParUnionFunc[String])
        val data = new ReflectTest(text)
        val path = HDMContext.leaderPath.get()+ "/"+ HDMContext.BLOCK_MANAGER_NAME
        val msg = (hdm.id, hdm.partitioner, hdm.func, hdm.blocks)
        SmsSystem.askMsg(path, hdm)
      }
    }.start
    Thread.sleep(60000)
  }

  @Test
  def testRegisterBlock(): Unit ={

    new Thread {
      override def run {
        HDMContext.init(leader ="akka.tcp://masterSys@127.0.0.1:8999/user/smsMaster")
        val hdm = new DFM[String,String](children = null, func = new ParUnionFunc[String])
        HDMContext.declareHdm(Seq(hdm))
        val ddm = HDM.horizontal(text, text2) // register through hdm constrcutor
      }
    }.start
    Thread.sleep(60000)
  }

  @Test
  def testClusterTaskRunning(): Unit ={

    new Thread {
      override def run {
        HDMContext.init(leader ="akka.tcp://masterSys@127.0.0.1:8999/user/smsMaster")
        Thread.sleep(1000)
//        val hdm = HDM.horizontal(text, text2)
        val path = Path("hdfs://127.0.0.1:9001/user/spark/benchmark/micro/rankings")
        val hdm = HDM(path)
        val wordCount = hdm.map(w => (w.substring(0,3),1)).groupReduce(_._1, (t1,t2) => (t1._1, t1._2 + t2._2))

        wordCount.compute(2) onComplete  {
          case Success(hdm) =>
            println("Job completed and received response:" + hdm)
            hdm.asInstanceOf[HDM[_,_]].sample().foreach(println(_))
          case Failure(t) =>
            println("Job failed because of: " + t)
            t.printStackTrace()
        }

      }
    }.start
    Thread.sleep(600000)
  }




  @After
  def afterTest(): Unit ={
    HDMContext.shutdown()
  }

}


class ReflectTest[ct:ClassTag] (elems:Seq[ct]) extends Serializable{

  val inType = classTag[ct]

  override def toString: String = {
    s"ReflectTest[${inType.toString}}]" + "\n" +
    s"elems:$elems"
  }
}