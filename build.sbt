name := "crate-scala"

organization := "io.crate"

version := "1.0"

scalaVersion := "2.11.4"

libraryDependencies ++= Seq(
  "io.crate" % "crate-client" % "0.45.5",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test"
)

resolvers += "Crate bintray" at "http://dl.bintray.com/crate/crate"

testOptions in Test += Tests.Argument("-oDF")