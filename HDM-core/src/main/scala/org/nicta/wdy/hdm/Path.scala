package org.nicta.wdy.hdm

/**
 * Created by Tiantian on 2014/5/26.
 */
trait Path extends Serializable{

}

case class RemotePath(val path: String) extends Path
