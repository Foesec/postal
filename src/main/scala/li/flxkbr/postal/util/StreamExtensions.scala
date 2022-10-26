package li.flxkbr.postal.util

import fs2.Stream

transparent trait StreamExtensions {

  extension [F[_], L, R](s: Stream[F, Either[L, R]])
    def collectRight: Stream[F, R] = s.collect { case Right(value) => value }

}
