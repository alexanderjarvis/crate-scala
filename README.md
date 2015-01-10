# Crate Scala

A Reactive Scala wrapper for the official [Crate](https://crate.io) client database driver in Java.

## Get started

Add the following dependency to your build.sbt

	"io.crate" %% "crate-scala" % "1.0"
	
## Usage

An example:
	
	val client = ReactiveCrateClient("localhost:4300")
	
	val request = client.sql("SELECT * FROM sys.nodes").map { response =>
		println("Crate Nodes: " + response.cols.length)
	}
	
	// Blocking for example only
	Await.result(request, 5 seconds)
	
See [CrateClientSpec](src/test/scala/CrateClientSpec.scala) for more working examples.

## Building

This project requires SBT to compile.

Tests require a Crate database on localhost.

	$ sbt clean compile test
	
## Publishing

Currently you can publish to your local ivy repo to include as a dependency.

	$ sbt publish-local