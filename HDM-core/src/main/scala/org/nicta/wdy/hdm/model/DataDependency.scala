package org.nicta.wdy.hdm.model

/**
 * Created by Tiantian on 2014/12/3.
 */
trait DataDependency extends Serializable

case object OneToOne extends DataDependency

case object NToN extends DataDependency

case object OneToN extends DataDependency

case object NToOne extends DataDependency

case object PartialNToOne extends DataDependency

case object FullNtoOne extends DataDependency



trait FuncDependency extends Serializable 

case object FullDep extends FuncDependency

case object PartialDep extends FuncDependency