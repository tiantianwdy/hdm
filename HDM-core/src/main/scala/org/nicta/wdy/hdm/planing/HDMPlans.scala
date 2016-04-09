package org.nicta.wdy.hdm.planing

import org.nicta.wdy.hdm.model.AbstractHDM

/**
 * Created by tiantian on 9/04/16.
 */
case class HDMPlans(logicalPlan:Seq[AbstractHDM[_]],
                    logicalPlanOpt:Seq[AbstractHDM[_]],
                    physicalPlan:Seq[AbstractHDM[_]]) extends Serializable{

}
