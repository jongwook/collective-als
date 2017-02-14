name := "collective-als"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq("provided", "test").map { config =>
  "com.kakao.cuesheet" %% "cuesheet" % "0.10.0" % config
} ++ Seq(
  "org.scalatest" %% "scalatest" % "3.0.0" % "test",
  "com.github.jongwook" %% "spark-ranking-metrics" % "0.0.1",
  "com.github.fommil.netlib" % "all" % "1.1.2" pomOnly()
)

