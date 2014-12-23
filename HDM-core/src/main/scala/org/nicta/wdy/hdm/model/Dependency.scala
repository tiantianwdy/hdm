package org.nicta.wdy.hdm.model

/**
 * Created by Tiantian on 2014/12/3.
 */
trait Dependency

case object OneToOne extends Dependency

case object NToN extends Dependency

case object OneToN extends Dependency

case object NToOne extends Dependency