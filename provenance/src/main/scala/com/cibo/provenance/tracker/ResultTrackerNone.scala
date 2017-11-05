package com.cibo.provenance.tracker

import com.cibo.provenance._

import scala.reflect.ClassTag

/**
  * Created by ssmith on 5/16/17.
  *
  * This null ResultTracker never saves anything, and always returns a value for a query
  * by running the function in question.
  */

case class ResultTrackerNone()(implicit val currentBuildInfo: BuildInfo) extends ResultTracker {

  def getCurrentBuildInfo = currentBuildInfo

  implicit val bi: BuildInfo = currentBuildInfo

  def saveResult[O](v: FunctionCallResultWithProvenance[O]): FunctionCallResultWithProvenanceDeflated[O] = {
    // a no-op that just calculates the ID and returns it
    FunctionCallResultWithProvenanceDeflated(v)(rt=this)
  }

  def saveCall[O](v: FunctionCallWithProvenance[O]): FunctionCallWithProvenanceDeflated[O] = {
    // a no-op that just calculates the ID and returns it
    FunctionCallWithProvenanceDeflated(v)(rt=this)
  }


  def hasResultFor[O](v: FunctionCallWithProvenance[O]): Boolean = true // always

  def loadResultForCallOption[O](f: FunctionCallWithProvenance[O]): Option[FunctionCallResultWithProvenance[O]] = {
    // just re-run anything we need to "load"
    Some(f.run(this))
  }

  def saveValue[T : ClassTag](obj: T): Digest = {
    // also a no-op that just calculates the ID and returns it
    Util.digestObject(obj)
  }

  def hasValue[T : ClassTag](obj: T): Boolean = false // never

  def hasValue(digest: Digest): Boolean = false // never

  def loadCallOption[O : ClassTag](className: String, version: Version, digest: Digest): Option[FunctionCallWithProvenance[O]] = None // never

  def loadValueOption[O : ClassTag](digest: Digest): Option[O] = None // never

  def loadValueSerializedDataOption(className: String, digest: Digest): Option[Array[Byte]] = None // never

}

object ResultTrackerNone {
  //def apply(implicit val buildInfo: BuildInfo) = new ResultTrackerNone(buildInfo)
}
