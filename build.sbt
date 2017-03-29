name := "collective-als"

scalaVersion := "2.11.8"

resolvers += "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

scalacOptions := Seq("-feature", "-unchecked", "-encoding", "utf8")

libraryDependencies ++= Seq(
  "com.kakao.cuesheet" %% "cuesheet" % "0.10.1-SNAPSHOT",
  "org.scalatest" %% "scalatest" % "3.0.0" % "test",
  "com.github.jongwook" %% "spark-ranking-metrics" % "0.0.1",
  "com.github.fommil.netlib" % "all" % "1.1.2" pomOnly()
)

