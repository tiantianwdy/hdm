package org.nicta.hdm.io

import org.nicta.wdy.hdm.executor.HDMContext
import org.nicta.wdy.hdm.functions.NullFunc
import org.nicta.wdy.hdm.io.{BufferedBlockIterator, Path}
import org.nicta.wdy.hdm.model.DDM

import org.junit.{Before, Test}
import org.nicta.wdy.hdm.storage.{Computed, Block}

/**
 * Created by tiantian on 7/10/15.
 */
class BufferIteratorTest {

 @Test
 def testBufferIteratorTest(): Unit = {
   val ddms = for (i <- 1 to 5) yield {
     val id = s"blk-00$i"
     new DDM[(String, List[Double]), (String, List[Double])](id= id,
       func = new NullFunc[(String, List[Double])],
       blockSize = 0,
       state = Computed,
       location = Path("hdm://tiantian-HP-EliteBook-Folio-9470m:9091/" + id))
   }
   val start = System.currentTimeMillis()
   val iterator = new BufferedBlockIterator[(String, List[Double])](ddms)
   println(iterator.head)
   println(iterator.size)
   val end = System.currentTimeMillis()
   println(s"${end - start} ms. ")
 }

}
