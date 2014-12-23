package org.nicta.wdy.hdm.storage

/**
 * Created by Tiantian on 2014/12/1.
 */
trait BlockState {

}

case object Declared extends BlockState

case object Computing extends BlockState

case object Computed extends BlockState

case object Removed extends BlockState
