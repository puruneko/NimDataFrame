# Package
name          = "nimdataframe"
version       = "0.1.0"
author        = "puruneko"
description   = "dirty implementation data frame library for Nim"
license       = "MIT"

srcDir = "src"
binDir = "bin"
skipDirs = "lab"

# Dependencies
requires "nim >= 1.0.0"

#task
task test, "Run unit test":
    exec "nim c -r test/unit.nim"