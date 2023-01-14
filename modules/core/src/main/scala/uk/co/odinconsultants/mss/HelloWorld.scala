package uk.co.odinconsultants.mss

import cats.effect.{IO, IOApp}

object HelloWorld extends IOApp.Simple {
  val HelloWorld = "Hello world"
  val printingHelloWorld: IO[String] = IO {
    println(HelloWorld)
    HelloWorld
  }
  def run: IO[Unit] = printingHelloWorld.void
}
