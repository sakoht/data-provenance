import de.heikoseeberger.sbtheader.HeaderPattern
headers := Map(
  "scala" -> (
    HeaderPattern.cStyleBlockComment,
    """/* Copyright (c) 2017 CiBO Technologies - All Rights Reserved
      | * You may use, distribute, and modify this code under the
      | * terms of the BSD 3-Clause license.
      | *
      | * A copy of the license can be found on the root of this repository,
      | * at https://github.com/cibotech/provenance/LICENSE.md,
      | * or at https://opensource.org/licenses/BSD-3-Clause
      | */
      |
      |""".stripMargin
    )
)
