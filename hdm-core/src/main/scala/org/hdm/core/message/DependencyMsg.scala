package org.hdm.core.message

/**
 * Created by tiantian on 4/03/16.
 */
trait DependencyMsg extends Serializable

/**
 * msg contains the third party dependency libs of a application
 * @param appName
 * @param version
 * @param dependencyName
 * @param content
 * @param author
 */
case class AddDependency(appName:String,
                             version:String,
                             dependencyName:String,
                             content:Array[Byte],
                             author:String) extends DependencyMsg

/**
 * msg contains the binary contents of an application/job.
 * @param appName
 * @param version
 * @param content
 * @param author
 */
case class AddApplication(appName:String,
                          version:String,
                          content:Array[Byte],
                          author:String) extends DependencyMsg

/**
 * msg to remove a lib from an application
 * @param appName
 * @param version
 * @param dependencyName
 */
case class RemoveDependency(appName:String,
                            version:String,
                            dependencyName:String) extends DependencyMsg