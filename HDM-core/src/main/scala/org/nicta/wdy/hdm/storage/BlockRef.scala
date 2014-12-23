package org.nicta.wdy.hdm.storage

import org.nicta.wdy.hdm.functions.SerializableFunction
import org.nicta.wdy.hdm.model.{Horizontal, Distribution}

/**
 * Created by Tiantian on 2014/12/1.
 */
/**
 *
 * @param id unique block id
 * @param typ
 * @param parents id of parents blocks
 * @param distribution
 * @param funcDepcy
 * @param state
 * @param blocks
 * @param locations
 */
case class BlockRef(id: String,
                    typ: String,
                    parents: Seq[String],
                    distribution: Distribution = Horizontal,
                    funcDepcy: SerializableFunction[_, _],
                    state: BlockState = Declared,
                    blocks: Seq[Block[_]] = null,
                    locations: Seq[String] = null) extends Serializable {


}
