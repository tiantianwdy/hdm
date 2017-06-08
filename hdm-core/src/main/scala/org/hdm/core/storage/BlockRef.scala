package org.hdm.core.storage

import org.hdm.core.functions.SerializableFunction
import org.hdm.core.model.{Horizontal, Distribution}
import scala.language.existentials

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
