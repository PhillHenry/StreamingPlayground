package uk.co.odinconsultants

import cats.effect.IO
import java.util.Date

object LoggingUtils {

  def ioLog(x: String): IO[Unit] = IO.println(toMessage(x))

  def toMessage(x: => String): String = s"PH ${new Date()}: $x"

}
