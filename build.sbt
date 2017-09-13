import sbt._

lazy val javacSeq = Seq(
  "-J-XX:MaxPermSize=256m",
  "-source", "1.8",
  "-target", "1.8"
)

lazy val groupids: Map[String, String] = Map(
  "akka" -> "com.typesafe.akka",
  "asm" -> "org.ow2.asm",
  "breeze" -> "org.scalanlp",
  "chill" -> "com.twitter",
  "hbase" -> "org.apache.hbase",
  "hadoopCore" -> "org.apache.hadoop",
  "hadoop" -> "org.apache.hadoop",
  "httpcomponents" -> "org.apache.httpcomponents",
  "jackson" -> "com.fasterxml.jackson",
  "jetty" -> "org.eclipse.jetty",
  "mortbay-jetty" -> "org.mortbay.jetty",
  "junit" -> "junit",
  "log4j" -> "log4j",
  "logback" -> "ch.qos.logback",
  "mahout" -> "org.apache.mahout",
  "mesos" -> "org.apache.mesos",
  "netty" -> "io.netty",
  "protobuf" -> "com.google.protobuf",
  "scala" -> "org.scala-lang",
  "slf4j" -> "org.slf4j",
  "snappy" -> "org.xerial.snappy",
  "spark" -> "org.apache.spark"
)

lazy val revisions: Map[String, String] = Map(
  "scala" -> "2.10.2",
  "scalaBinary" -> "2.10",
  "scalaTools" -> "2.10",
  "akka" -> "2.3.8",
  "asm" -> "4.0",
  "breeze" -> "0.11.1",
  "chill" -> "0.9.1",
  "hbase" -> "0.94.6",
  "hadoopCore" -> "1.2.1",
  "hadoop" -> "2.6.0",
  "httpclient" -> "4.5.3",
  "jackson" -> "2.2.2",
  "jetty" -> "8.1.14.v20131031",
  "junit" -> "4.10",
  "log4j" -> "1.2.17",
  "logback" -> "1.0.13",
  "mahout" -> "0.9",
  "mesos" -> "0.13.0",
  "netty" -> "4.0.23.Final",
  "protobuf" -> "2.5.0",
  "slf4j" -> "1.7.2",
  "snappy" -> "1.1.1.6",
  "spark" -> "0.9.1"
)

lazy val akkaDep: Seq[ModuleID] = for (n <- Seq("akka-actor_", "akka-remote_", "akka-slf4j_", "akka-testkit_"))
  yield groupids("akka") % (n + revisions("scalaTools")) % revisions("akka")

lazy val asmDep: Seq[ModuleID] =
  for (n <- Seq("", "-commons", "-tree"))
    yield groupids("asm") % ("asm" + n) % revisions("asm")

lazy val jacksonDep: Seq[ModuleID] = Seq(
  "com.fasterxml.jackson.core" % "jackson-databind" % revisions("jackson"),
  "com.fasterxml.jackson.module" % ("jackson-module-scala_" + revisions("scalaTools")) % revisions("jackson")
    excludeAll(
    ExclusionRule(organization = "org.scala-lang", name = "scala-library"),
    ExclusionRule(organization = "org.scalatest", name = "scalatest_" + revisions("scalaTools")),
    ExclusionRule(organization = "com.google.guava", name = "guava")
    )
)

lazy val jettyDep: Seq[ModuleID] =
  for (n <- Seq("server", "servlet", "security", "webapp"))
    yield groupids("jetty") % ("jetty-" + n) % revisions("jetty")

lazy val logbackDep: Seq[ModuleID] =
  for (n <- Seq("classic", "core"))
    yield groupids("logback") % ("logback-" + n) % revisions("logback")

lazy val commonSettings = Seq(
  organization := "org.hdm",
  version := "0.0.1",
  scalaVersion := "2.10.2",
  publishArtifact in(Compile, packageBin) := true,
  artifact in(Compile, packageBin) := {
    val prev: Artifact = (artifact in(Compile, packageBin)).value
    prev.copy(`type` = "jar", extension = "jar")
  },
  artifactName := {
    (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
      artifact.name + "-" + module.revision + "." + artifact.extension
  },
  crossTarget in(Compile, packageBin) <<= baseDirectory {
    _ / "target/scala"
  }
)

lazy val coreDep: Seq[ModuleID] = Seq(
  groupids("junit") % "junit" % revisions("junit"),
  groupids("protobuf") % "protobuf-java" % revisions("protobuf"),
  groupids("snappy") % "snappy-java" % revisions("snappy"),
  groupids("chill") % ("chill_" + revisions("scalaBinary")) % revisions("chill") excludeAll(
    ExclusionRule(organization = groupids("asm"), name = "asm"),
    ExclusionRule(organization = groupids("asm"), name = "asm-commons")
    ),
  groupids("chill") % "chill-java" % revisions("chill") excludeAll(
    ExclusionRule(organization = groupids("asm"), name = "asm"),
    ExclusionRule(organization = groupids("asm"), name = "asm-commons")
    ),
  groupids("netty") % "netty-all" % revisions("netty"),
  groupids("mahout") % "mahout-math-scala" % revisions("mahout"),
  groupids("breeze") % ("breeze_" + revisions("scalaBinary")) % revisions("breeze"),
  groupids("scala") % "scala-library" % revisions("scala"),
  groupids("scala") % "scala-reflect" % revisions("scala")
)


lazy val engineDep: Seq[ModuleID] = Seq(
  groupids("junit") % "junit" % revisions("junit"),
  groupids("protobuf") % "protobuf-java" % revisions("protobuf"),
  groupids("snappy") % "snappy-java" % revisions("snappy"),
  groupids("chill") % ("chill_" + revisions("scalaBinary")) % revisions("chill") excludeAll(
    ExclusionRule(organization = groupids("asm"), name = "asm"),
    ExclusionRule(organization = groupids("asm"), name = "asm-commons")
  ),
  groupids("chill") % "chill-java" % revisions("chill") excludeAll(
    ExclusionRule(organization = groupids("asm"), name = "asm"),
    ExclusionRule(organization = groupids("asm"), name = "asm-commons")
  ),
  groupids("hadoop") % "hadoop-client" % revisions("hadoop") excludeAll(
    ExclusionRule(organization = groupids("asm"), name = "asm"),
    ExclusionRule(organization = "asm", name = "asm"),
    ExclusionRule(organization = groupids("netty"), name = "netty"),
    ExclusionRule(organization = groupids("protobuf"), name = "protobuf-java"),
    ExclusionRule(organization = groupids("mortbay-jetty"), name = "servlet-api-2.5"),
    ExclusionRule(organization = groupids("junit"), name = "junit")
  ),
  groupids("httpcomponents") % "httpclient" % revisions("httpclient"),
  groupids("scala") % "scala-library" % revisions("scala"),
  groupids("scala") % "scala-reflect" % revisions("scala")
)

lazy val akkaDistDep: Seq[ModuleID] = Seq(
  groupids("junit") % "junit" % revisions("junit")
) ++ akkaDep

lazy val akkaDist = (project in file("./akka-distribution"))
  .settings(commonSettings: _*)
  .settings(
    name := "akka-distribution",
    libraryDependencies ++= akkaDistDep
  )

lazy val root = (project in file("."))
  .aggregate(akkaDist, console, core)
  .settings(commonSettings: _*)
  .settings(
    name := "hdm-cluster",
    resolvers += Resolver.sonatypeRepo("public"),
    javacOptions ++= javacSeq,
    scalacOptions += "-target:jvm-1.8",
    mainClass in assembly := Some("Main"),
    assemblyJarName in assembly := ""
  )

lazy val core = (project in file("./hdm-core"))
  .settings(commonSettings: _*)
  .settings(
    name := "hdm-core",
    libraryDependencies ++= coreDep ++ akkaDep ++ logbackDep ++ asmDep
  )
  .dependsOn(akkaDist)

lazy val engine = (project in file("./hdm-engine"))
  .settings(commonSettings: _*)
  .settings(
    name := "hdm-engine",
    libraryDependencies ++= engineDep ++ akkaDep ++ logbackDep ++ asmDep
  )
  .dependsOn(core)

lazy val consoleDep: Seq[ModuleID] = jettyDep ++ jacksonDep

lazy val console = (project in file("./hdm-console"))
  .settings(commonSettings: _*)
  .settings(
    name := "hdm-console",
    libraryDependencies ++= consoleDep
  )
  .dependsOn(engine)
