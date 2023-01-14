package uk.co.odinconsultants.mss

import weaver.FunSuite
import weaver.scalacheck.Checkers
import cats.effect.IO
import cats.data.NonEmptyList

object HelloWorldSuite extends FunSuite with Checkers:
  test("hello world happy path") {
    NonEmptyList
      .of(
        expect.same(HelloWorld.run, HelloWorld.run)
      ).reduce
  }
