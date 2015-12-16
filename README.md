# Squid

SQL parser and type checker for Scala.

## Prerequisites

First, you'll need to create an **.sbtopts** file in the root of the project which
defines some arguments to the JVM -

* **java.library.path** - This should always be set to `parser/target/native/bin`
* **JAVA_HOME** - This should be set to your installed JDK.  The directory should
    have the `include/` and `include/linux` directories required for compiling
    native code.
* **LIBPG_QUERY_DIR** - This directory should contain the headers and compiled
    `libpg_query.a` library required for compiling native code.  It's probably
    simplest to clone [libpg_query](https://github.com/lfittl/libpg_query) somewhere
    and `make` it.

Here's a full example of an **.sbtopts** file -

```
-Djava.library.path=parser/target/native/bin
-DJAVA_HOME=/usr/lib/jvm/java-8-oracle
-DLIBPG_QUERY_DIR=path/to/libpg_query
```
