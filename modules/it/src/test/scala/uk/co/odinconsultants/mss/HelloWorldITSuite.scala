package uk.co.odinconsultants.mss

import cats.data.NonEmptyList
import cats.effect.IO
import weaver.FunSuite
import weaver.scalacheck.Checkers

object HelloWorldITSuite extends FunSuite with Checkers:
  test("hello world happy path (IT)") {
    NonEmptyList
      .of(
        expect.same(HelloWorld.run, HelloWorld.run)
      ).reduce
  }
