package com.cibo.provenance.exceptions

import com.cibo.provenance._

/**
  * Created by ssmith on 9/20/17.
  */

class InconsistentVersionException(
  functionName: String,
  version: Version,
  commits: Seq[String],
  inputGroupIdOption: Option[Digest]
) extends RuntimeException(
  f"Function $functionName at version $version has inconsistent results at commits: $commits"
    + f" (inputs: $inputGroupIdOption)"
)

class InvalidVersionException[O](
  requestedVersion: Version,
  function: FunctionWithProvenance[O],
  msg: String
) extends RuntimeException(msg)


class UnknownVersionException[O](
  requestedVersion: Version,
  function: FunctionWithProvenance[O],
  msg: String
) extends InvalidVersionException(requestedVersion, function, msg)


class UnrunnableVersionException[O](
  requestedVersion: Version,
  function: FunctionWithProvenance[O],
  msg: String
) extends InvalidVersionException(requestedVersion, function, msg)


object InvalidVersionException {
  def apply[O](v: Version, f: FunctionWithProvenance[O]): InvalidVersionException[O] = {
    new InvalidVersionException(
      requestedVersion = v,
      function = f,
      f"Cannot run version $v of $f!  Supported versions are ${f.runnableVersions}"
    )
  }
}

object UnknownVersionException {
  def apply[O](v: Version, f: FunctionWithProvenance[O]): UnknownVersionException[O] = {
    new UnknownVersionException[O](
      requestedVersion = v,
      function = f,
      f"Unrecognized version $v of $f!  Known versions are ${f.loadableVersions}"
    )
  }
}

object UnrunnableVersionException {
  def apply[O](v: Version, f: FunctionWithProvenance[O]): UnrunnableVersionException[O] = {
    new UnrunnableVersionException[O](
      requestedVersion = v,
      function = f,
      f"Version $v of $f is not runnable!  Runnable versions are ${f.runnableVersions}.  Override implVersion to support running alternate vesions."
    )
  }
}
