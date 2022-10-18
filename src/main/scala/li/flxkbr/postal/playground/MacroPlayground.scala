package li.flxkbr.postal.playground

import scala.quoted.*
import scala.annotation.tailrec

object MacroPlayground {

  def inspectCode(x: Expr[Any])(using Quotes): Expr[Any] =
    println(x.show)
    x

  inline def inspect(inline x: Any): Any = ${ inspectCode('x) }

  def powerCode(
      x: Expr[Double],
      n: Expr[Int],
  )(using Quotes): Expr[Double] =
    import quotes.reflect.report
    (x.value, n.value) match
      case (Some(base), Some(exponent)) =>
        val value: Double = pow(base, exponent)
        Expr(value)
      case (Some(_), _) =>
        report.errorAndAbort(
          "Expected a known value for the exponent, but was " + n.show,
          n,
        )
      case _ =>
        report.errorAndAbort(
          "Expected a known value for the base, but was " + x.show,
          x,
        )

  def pow(x: Double, n: Int): Double =
    if n == 0 then 1 else x * pow(x, n - 1)

  inline def power(inline x: Double, inline n: Int) = ${ powerCode('x, 'n) }
}
