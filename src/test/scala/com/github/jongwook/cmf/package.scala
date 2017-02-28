package com.github.jongwook

package object cmf {
  case class LiveThumb(profile_id: Int, content_id: Int, thumb: Float, timestamp: Long)

  case class DimTrack(content_id: Int, artist_id: Int, weight: Float)
}
