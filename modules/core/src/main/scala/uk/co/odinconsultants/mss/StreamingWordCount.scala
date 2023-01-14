package uk.co.odinconsultants.mss

import fs2.Stream
import cats.effect.{IO, Ref}
import uk.co.odinconsultants.mss.StreamingWordCount.WordCount

object StreamingWordCount {

  type WordCount = Map[String, Int]

  def update(old: WordCount, word: String): WordCount = old + (word -> (old.getOrElse(word, 0) + 1))

  def updateIO(ref: Ref[IO, WordCount], word: String): IO[WordCount] = for {
    _     <- IO.println(word)
    _     <- ref.update(old => update(old, word))
    count <- ref.get
  } yield count

  def orderedStream(xs: List[String]): Stream[IO, WordCount] = {
    val initial: IO[Ref[IO, WordCount]] = Ref[IO].of(Map.empty[String, Int])
    for {
      ref    <- Stream.eval(initial)
      word   <- Stream.emits(xs)
      result <- Stream.eval(updateIO(ref, word))
    } yield result
  }

}
