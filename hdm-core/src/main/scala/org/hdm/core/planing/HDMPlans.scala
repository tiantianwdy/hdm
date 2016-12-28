package org.hdm.core.planing

import org.hdm.core.model.HDM

/**
 * Created by tiantian on 9/04/16.
 */
case class HDMPlans(logicalPlan:Seq[HDM[_]],
                    logicalPlanOpt:Seq[HDM[_]],
                    physicalPlan:Seq[HDM[_]]) extends Serializable{

}

case class MultiClusterPlans(remoteJobs:Seq[HDM[_]],
                             localJobs:Seq[HDM[_]])  extends Serializable{
}

