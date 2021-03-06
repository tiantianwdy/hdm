package org.hdm.core.executor

import org.hdm.akka.server.SmsSystem
import org.hdm.core.context.HDMContext
import org.hdm.core.functions.ParUnionFunc
import org.hdm.core.io.Path
import org.hdm.core.model.{DFM, HDM}
import org.junit.{Ignore, After, Before, Test}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.reflect.{ClassTag, classTag}
import scala.util.{Failure, Success}

/**
 * Created by tiantian on 3/01/15.
 */
class HDMClusterTest extends ClusterTestSuite {

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
    hDMContext.clusterExecution.set(false)
    appContext.setMasterPath("akka.tcp://masterSys@127.0.1.1:8999/user/smsMaster")
    hDMEntry.init() // start master
  }

  @Ignore
  @Test
  def testStartFollower(): Unit ={
    new Thread {
      override def run {
        hDMEntry.init(leader ="akka.tcp://masterSys@127.0.0.1:8999/user/smsMaster")
      }
    }.start
    Thread.sleep(3000)
  }

  @Test
  def testSendSerializableMsg(): Unit ={
    new Thread {
      override def run {
//        hDMContext.init(leader ="akka.tcp://masterSys@127.0.0.1:8999/user/smsMaster")
        val hdm = new DFM[String,String](children = null, func = new ParUnionFunc[String], location = Path(hDMContext.clusterBlockPath), appContext = appContext)
        val data = new ReflectTest(text)
        val path = hDMContext.leaderPath.get()+ "/"+ HDMContext.BLOCK_MANAGER_NAME
        val msg = (hdm.id, hdm.partitioner, hdm.func, hdm.blocks)
        SmsSystem.askSync(path, hdm)
      }
    }.start
    Thread.sleep(3000)
  }

  @Test
  def testRegisterBlock(): Unit ={

    new Thread {
      override def run {
//        hDMContext.init(leader ="akka.tcp://masterSys@127.0.0.1:8999/user/smsMaster")
        val hdm = new DFM[String,String](children = null, func = new ParUnionFunc[String], location = Path(hDMContext.clusterBlockPath), appContext = appContext)
        hDMEntry.declareHdm(Seq(hdm))
        val ddm = HDM.horizontal(appContext, hDMContext, text, text2) // register through hdm constrcutor
      }
    }.start
    Thread.sleep(3000)
  }

  @Test
  def testClusterTaskRunning(): Unit ={

    new Thread {
      override def run {
//        hDMContext.init(leader ="akka.tcp://masterSys@127.0.0.1:8999/user/smsMaster")
        Thread.sleep(1000)
        val hdm = HDM.horizontal(appContext, hDMContext, text, text2)
//        val path = Path("hdfs://127.0.0.1:9001/user/spark/benchmark/micro/rankings")
//        val hdm = HDM(path, appContext, hDMContext)
        // test programs
        val wordCount = hdm.map{ w =>
            val as = w.split(" ")
            (as(0), 1)
        }.groupReduce(t => t._1, (t1,t2) => (t1._1,t1._2 + t2._2) )
          //.map(w => (w.split(","))).map(as => (as(0).substring(0,3),as(1).toInt)).groupReduce(_._1, (t1,t2) => (t1._1, t1._2 + t2._2))

        val topK = hdm.map{ w =>
          val as = w.split(" ")
          as(1).toInt
        }.top(10)

        val count = hdm.count()

        val start = System.currentTimeMillis()
        wordCount.compute(1, hDMEntry) onComplete  {
          case Success(hdm) =>
            println(s"Job completed in ${System.currentTimeMillis()- start} ms. And received response: ${hdm.id}")
//            hdm.asInstanceOf[HDM[_,_]].sample(10).foreach(println(_))
          case Failure(t) =>
            println("Job failed because of: " + t)
            t.printStackTrace()
            System.exit(1)
        }

      }
    }.start
    Thread.sleep(10000)
  }




  @After
  def afterTest(): Unit ={
    hDMEntry.shutdown()
  }

}


class ReflectTest[ct:ClassTag] (elems:Seq[ct]) extends Serializable{

  val inType = classTag[ct]

  override def toString: String = {
    s"ReflectTest[${inType.toString}}]" + "\n" +
    s"elems:$elems"
  }
}