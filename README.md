# SwayDB.kotlin

[![Maven Central](https://img.shields.io/maven-central/v/com.github.javadev/swaydb-kotlin.svg)](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22com.github.javadev%22%20AND%20a%3A%22swaydb-kotlin%22)
[![Build Status](https://travis-ci.com/simerplaha/SwayDB.kotlin.svg?branch=master)](https://travis-ci.com/simerplaha/SwayDB.kotlin)
[![codecov.io](http://codecov.io/github/simerplaha/SwayDB.kotlin/coverage.svg?branch=master)](http://codecov.io/github/simerplaha/SwayDB.kotlin?branch=master)

Kotlin wrapper for [SwayDB](https://github.com/simerplaha/SwayDB).

Requirements
============

Kotlin 1.3 and later.

## Installation

Include the following in your `pom.xml` for Maven:

```
<dependencies>
  <dependency>
    <groupId>com.github.javadev</groupId>
    <artifactId>swaydb-kotlin</artifactId>
    <version>0.8-beta.8.2</version>
  </dependency>
  ...
</dependencies>
```

Gradle:

```groovy
compile 'com.github.javadev:swaydb-kotlin:0.8-beta.8.2'
```

### Usage

```kotlin
import org.hamcrest.CoreMatchers.equalTo
import org.junit.Assert.assertThat
import org.junit.Test
import java.util.AbstractMap.SimpleEntry

class QuickStartTest {

    @Test
    fun quickStart() {
        // Create a memory database
        // val db = memory.Map[Int, String]().get
        swaydb.kotlin.memory.Map.create<Int, String>(
                Int::class, String::class).use { db ->
        
                    // write 100 key-values atomically
                    db.put((1..100)
                            .map { index -> SimpleEntry<Int, String>(index, index.toString()) }
                            .map { it.key to it.value }
                            .toMap().toMutableMap())
        
                    // Iteration: fetch all key-values withing range 10 to 90, update values
                    // and atomically write updated key-values
                    db
                            .from(10)
                            .takeWhile({ item -> item.key <= 90 })
                            .map({ item -> item.setValue(item.value + "_updated"); item })
                            .materialize()
                            .foreach({entry -> db.put(entry)})
        
                    // assert the key-values were updated
                    (10..90)
                            .map { item -> SimpleEntry<Int, String>(item, db.get(item)) }
                            .forEach { pair -> assertThat(pair.value.endsWith("_updated"), equalTo(true)) }
                }
    }
}
```
