package com.github.jongwook.cmf

import com.kakao.cuesheet.CueSheet

import scala.concurrent.duration._
import scala.util.Random

object Streaming extends CueSheet {
  ssc.generated(10.milliseconds)(Random.nextDouble).print()
}
