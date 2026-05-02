package com.sparkling.frame

/** Spark join strategy passed to `Frame.join`.
  *
  * Maps to Spark's join type strings:
  *   - `Inner` — keep only rows that match on both sides
  *   - `Left` — keep all left rows; unmatched right columns are null
  *   - `Right` — keep all right rows; unmatched left columns are null
  *   - `Outer` — keep all rows from both sides; unmatched columns are null
  *   - `LeftSemi` — keep left rows that have at least one match on the right; right columns are not included
  *   - `LeftAnti` — keep left rows that have no match on the right; right columns are not included
  */
sealed trait JoinType {
  private[frame] def sparkName: String
}

object JoinType {
  case object Inner extends JoinType { val sparkName = "inner" }
  case object Left extends JoinType { val sparkName = "left" }
  case object Right extends JoinType { val sparkName = "right" }
  case object Outer extends JoinType { val sparkName = "outer" }
  case object LeftSemi extends JoinType { val sparkName = "leftsemi" }
  case object LeftAnti extends JoinType { val sparkName = "leftanti" }
}
