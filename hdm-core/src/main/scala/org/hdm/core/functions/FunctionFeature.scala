package org.hdm.core.functions

/**
 * Created by tiantian on 2/03/16.
 */
trait FunctionFeature extends Serializable

case object Aggregation extends  FunctionFeature

case object Pruning extends FunctionFeature

case object PureParallel extends FunctionFeature