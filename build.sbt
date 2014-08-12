name := "crate-scala"

organization := "io.crate"

version := "1.0"

scalaVersion := "2.11.1"

libraryDependencies ++= Seq(
  "io.crate" % "crate-client" % "0.41.2",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test"
)

resolvers += "Crate bintray" at "http://dl.bintray.com/crate/crate"

testOptions in Test += Tests.Argument("-oDF")