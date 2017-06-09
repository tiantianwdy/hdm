package org.hdm.core.scheduling

import org.junit.Test

/**
 * Created by tiantian on 28/04/15.
 */
class SchedulerTest extends SchedulingTestData{




  @Test
  def testFindFreestWorker(): Unit ={
    Scheduler.getFreestWorkers(candidateMap) foreach(println(_))
    Scheduler.getFreestWorkers(partialCandidateMap) foreach(println(_))
    Scheduler.getFreestWorkers(nullCandidateMap) foreach(println(_))
  }
}
