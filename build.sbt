name := "collective-als"

scalaVersion := "2.11.8"

resolvers += "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

libraryDependencies ++= Seq("provided", "test").map { config =>
  "org.apache.spark" %% "spark-mllib" % "2.0.1" % config
} ++ Seq(
  "org.scalatest" %% "scalatest" % "3.0.0" % "test",
  "com.github.jongwook" %% "spark-ranking-metrics" % "0.1.0-SNAPSHOT",
  "com.github.fommil.netlib" % "all" % "1.1.2" pomOnly()
)

