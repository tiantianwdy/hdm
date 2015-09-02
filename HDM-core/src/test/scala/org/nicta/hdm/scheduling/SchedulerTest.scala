package org.nicta.hdm.scheduling

import java.util.concurrent.atomic.AtomicInteger

import org.junit.Test
import org.nicta.wdy.hdm.scheduling.Scheduler

import scala.collection.mutable

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
