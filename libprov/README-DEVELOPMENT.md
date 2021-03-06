Data Provenance Development
===========================

### Compiling and IntelliJ

This project uses sbt-boilerplate.  It will confuse IntelliJ briefly.

It generates all classes like Function{1..21}*:
- from: `src/main/boilarplate/**/*.scala.template`
- into: `target/scala-2.{11,12}/src_managed/**/*.scala`

If you are running IntelliJ and not having IntelliJ use sbt
to compile, it will find all of the template-based classes are missing
until after the first time you do `sbt +compile`.

If you modify any of the `.template` files, do another `sbt +compile` before resuming in IntelliJ.

### Testing

Since all content either matches the SHA1 at the end of the file path, or an empty file, the tests diff
results by comparing a directory _listing_ intead of content.  These live in:

    `src/test/resources/expected-output/scala-2.{11,12}/$TESTLABEL.manifest`

Most test cases use `TestUtils.testOutputBaseDir` to determine where store test output.
This is a directory under /tmp, with a subdirectory based on the USER environment variable.
When the test completes, its manifest is created and compared to the one in the above reference directory.

NOTE: If the test _fails_, the new manifest is put in the directory to replace it.  This means:
1. You can use git diff to see the difference after the failure.
2. You can just commit the new file if the change is intentional.
3. Arbitrary changes will fail the first time, pass the second time.
4. Don't commit the expected-output change w/o looking at it.


### Publishing

This project is released as a library.
- To test locally, use `sbt +publishLocal`.
- To do a real release, use `sbt release` (requires CiBO credentials)

