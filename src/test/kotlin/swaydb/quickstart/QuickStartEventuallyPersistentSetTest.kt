/*
 * Copyright (C) 2019 Simer Plaha (@simerplaha)
 *
 * This file is a part of SwayDB.
 *
 * SwayDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * SwayDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */
package swaydb.quickstart

import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import java.util.ArrayList
import java.util.Arrays
import java.util.HashSet
import java.util.concurrent.TimeUnit
import org.awaitility.Awaitility.await
import org.hamcrest.CoreMatchers.equalTo
import org.hamcrest.CoreMatchers.nullValue
import org.junit.Assert.assertThat
import org.junit.Test
import scala.collection.Seq
import scala.runtime.`Nothing$`
import swaydb.Prepare
import swaydb.base.TestBase
import swaydb.data.config.Dir
import java.nio.file.Paths
import java.time.Duration
import swaydb.data.config.MMAP
import swaydb.data.config.RecoveryMode
import java.io.IOException
import org.junit.BeforeClass

class QuickStartEventuallyPersistentSetTest : TestBase() {

    companion object {
        @BeforeClass
        @JvmStatic
        @Throws(IOException::class)
        fun beforeClass() {
            TestBase().deleteDirectoryWalkTreeStartsWith("target/disk4")
        }
    }

    @Test
    fun persistentSetInt() {
        // Create a persistent set database.
        // val db = eventually.persistent.Set[Int].get

        val db = swaydb.kotlin.eventually.persistent.Set.create<Int>(
                Int::class, addTarget(Paths.get("disk4")))
        // db.add(1).get
        db.add(1)
        // db.get(1).get
        val result = db.contains(1)
        assertThat("result contains value", result, equalTo(true))
        // db.remove(1).get
        db.remove(1)
        val result2 = db.contains(1)
        assertThat("Empty result", result2, equalTo(false))

        db.commit(
                swaydb.kotlin.Prepare.put(2, null),
                swaydb.kotlin.Prepare.remove(1) as Prepare<Int, `Nothing$`>
        )

        assertThat("two value", db.contains(2), equalTo(true))
        assertThat(db.contains(1), equalTo(false))
    }

    @Test
    fun persistentSetIntIterator() {
        val db = swaydb.kotlin.eventually.persistent.Set
                .builder<Int>()
                .withDir(addTarget(Paths.get("disk4iterator")))
                .withKeySerializer(Int::class)
                .build()
        db.add(1)
        assertThat(db.iterator().next(), equalTo(1))
    }

    @Test
    fun persistentSetIntToArray() {
        val db = swaydb.kotlin.eventually.persistent.Set
                .builder<Int>()
                .withDir(addTarget(Paths.get("disk4toArray")))
                .withKeySerializer(Int::class)
                .build()
        db.add(1)
        assertThat(Arrays.toString(db.toArray()), equalTo("[1]"))
    }

    @Test
    fun persistentSetIntAddExpireAfter() {
        swaydb.kotlin.eventually.persistent.Set
                .builder<Int>()
                .withDir(addTarget(Paths.get("disk4addExpireAfter")))
                .withKeySerializer(Int::class)
                .build().use { db ->
                    db.add(1, 100, TimeUnit.MILLISECONDS)
                    assertThat(db.contains(1), equalTo(true))
                    await().atMost(1800, TimeUnit.MILLISECONDS).until {
                        assertThat(db.contains(1), equalTo(false))
                        true
                    }
                }
    }

    @Test
    fun persistentSetIntAddExpireAt() {
        swaydb.kotlin.eventually.persistent.Set
                .builder<Int>()
                .withDir(addTarget(Paths.get("disk4addExpireAt")))
                .withKeySerializer(Int::class)
                .build().use { db ->
                    db.add(1, LocalDateTime.now().plusNanos(TimeUnit.MILLISECONDS.toNanos(100)))
                    assertThat(db.contains(1), equalTo(true))
                    await().atMost(1800, TimeUnit.MILLISECONDS).until {
                        assertThat(db.contains(1), equalTo(false))
                        true
                    }
                }
    }

    @Test
    fun persistentSetIntExpireAfter() {
        swaydb.kotlin.eventually.persistent.Set
                .builder<Int>()
                .withDir(addTarget(Paths.get("disk4expireAfter")))
                .withKeySerializer(Int::class)
                .build().use { db ->
                    db.add(1)
                    db.expire(1, 100, TimeUnit.MILLISECONDS)
                    assertThat(db.contains(1), equalTo(true))
                    await().atMost(1800, TimeUnit.MILLISECONDS).until {
                        assertThat(db.contains(1), equalTo(false))
                        true
                    }
                }
    }

    @Test
    fun persistentSetIntExpireAt() {
        swaydb.kotlin.eventually.persistent.Set
                .builder<Int>()
                .withDir(addTarget(Paths.get("disk4expireAt")))
                .withKeySerializer(Int::class)
                .build().use { db ->
                    db.add(1)
                    db.expire(1, LocalDateTime.now().plusNanos(TimeUnit.MILLISECONDS.toNanos(100)))
                    assertThat(db.contains(1), equalTo(true))
                    await().atMost(1800, TimeUnit.MILLISECONDS).until {
                        assertThat(db.contains(1), equalTo(false))
                        true
                    }
                }
    }

    @Test
    fun persistentSetIntContainsAll() {
        swaydb.kotlin.eventually.persistent.Set
                .builder<Int>()
                .withDir(addTarget(Paths.get("disk4containsAll")))
                .withKeySerializer(Int::class)
                .build().use { db ->
                    db.add(1)
                    assertThat(db.containsAll(Arrays.asList(1)), equalTo(true))
                }
    }

    @Test
    fun persistentSetIntAddAll() {
        swaydb.kotlin.eventually.persistent.Set
                .builder<Int>()
                .withDir(addTarget(Paths.get("disk4addAll")))
                .withKeySerializer(Int::class)
                .build().use { db ->
                    db.add(1)
                    db.add(Arrays.asList(2))
                    assertThat(db.containsAll(Arrays.asList(1, 2)), equalTo(true))
                }
    }

    @Test
    fun persistentSetIntRetainAll() {
        swaydb.kotlin.eventually.persistent.Set
                .builder<Int>()
                .withDir(addTarget(Paths.get("disk4retailAll")))
                .withKeySerializer(Int::class)
                .build().use { db ->
                    db.add(1)
                    db.add(Arrays.asList(2))
                    db.retainAll(Arrays.asList(1))
                    assertThat(db.containsAll(Arrays.asList(1)), equalTo(true))
                    db.retainAll(Arrays.asList(3))
                    assertThat(db.containsAll(Arrays.asList(1)), equalTo(false))
                }
    }

    @Test
    fun persistentSetIntRetainAll2() {
        swaydb.kotlin.eventually.persistent.Set
                .builder<String>()
                .withDir(addTarget(Paths.get("disk4retainAll2")))
                .withKeySerializer(String::class)
                .build().use { boxes ->
                    val bags = ArrayList<String>()
                    bags.add("pen")
                    bags.add("pencil")
                    bags.add("paper")

                    boxes.add("pen")
                    boxes.add("paper")
                    boxes.add("books")
                    boxes.add("rubber")

                    boxes.retainAll(bags)
                    assertThat(Arrays.toString(boxes.toArray()), equalTo("[paper, pen]"))
                }
    }

    @Test
    fun persistentSetIntRemove() {
        swaydb.kotlin.eventually.persistent.Set
                .builder<Int>()
                .withDir(addTarget(Paths.get("disk4remove")))
                .withKeySerializer(Int::class)
                .build().use { db ->
                    db.add(1)
                    db.add(2)
                    db.remove(HashSet(Arrays.asList(1, 2)))
                    assertThat(Arrays.toString(db.toArray()), equalTo("[]"))
                    db.add(3)
                    db.add(4)
                    db.remove(3, 4)
                    assertThat(Arrays.toString(db.toArray()), equalTo("[]"))
                }
    }

    @Test
    fun persistentSetIntSize() {
        swaydb.kotlin.eventually.persistent.Set
                .builder<Int>()
                .withDir(addTarget(Paths.get("disk4size")))
                .withKeySerializer(Int::class)
                .build().use { db ->
                    db.add(1)
                    db.add(Arrays.asList(2))
                    assertThat(db.size(), equalTo(2))
                    db.remove(1)
                    assertThat(db.size(), equalTo(1))
                }
    }

    @Test
    fun persistentSetIntIsEmpty() {
        swaydb.kotlin.eventually.persistent.Set
                .builder<Int>()
                .withDir(addTarget(Paths.get("disk4isEmpty")))
                .withKeySerializer(Int::class)
                .build().use { db ->
                    assertThat(db.isEmpty, equalTo(true))
                    db.add(1)
                    assertThat(db.isEmpty, equalTo(false))
                }
    }

    @Test
    fun persistentSetIntNonEmpty() {
        swaydb.kotlin.eventually.persistent.Set
                .builder<Int>()
                .withDir(addTarget(Paths.get("disk4nonEmpty")))
                .withKeySerializer(Int::class)
                .build().use { db ->
                    assertThat(db.nonEmpty(), equalTo(false))
                    db.add(1)
                    assertThat(db.nonEmpty(), equalTo(true))
                }
    }

    @Test
    fun persistentSetIntExpiration() {
        swaydb.kotlin.eventually.persistent.Set
                .builder<Int>()
                .withDir(addTarget(Paths.get("disk4expiration")))
                .withKeySerializer(Int::class)
                .build().use { db ->
                    val expireAt = LocalDateTime.now().plusNanos(TimeUnit.MILLISECONDS.toNanos(100))
                    db.add(1, expireAt)
                    assertThat(db.expiration(1)!!.truncatedTo(ChronoUnit.SECONDS).toString(),
                            equalTo(expireAt.truncatedTo(ChronoUnit.SECONDS).toString()))
                    assertThat<LocalDateTime>(db.expiration(2), nullValue())
                }
    }

    @Test
    fun persistentSetIntTimeLeft() {
        swaydb.kotlin.eventually.persistent.Set
                .builder<Int>()
                .withDir(addTarget(Paths.get("disk4timeLeft")))
                .withKeySerializer(Int::class)
                .build().use { db ->
                    val expireAt = LocalDateTime.now().plusNanos(TimeUnit.MILLISECONDS.toNanos(100))
                    db.add(1, expireAt)
                    assertThat(db.timeLeft(1)!!.seconds, equalTo(0L))
                    assertThat<Duration>(db.timeLeft(2), nullValue())
                }
    }

    @Test
    fun persistentSetIntSizes() {
        swaydb.kotlin.eventually.persistent.Set
                .builder<Int>()
                .withDir(addTarget(Paths.get("disk4sizes")))
                .withKeySerializer(Int::class)
                .build().use { db ->
                    db.add(1)
                    assertThat(db.sizeOfSegments(), equalTo(0L))
                    assertThat(db.level0Meter().currentMapSize(), equalTo(4000000L))
                    assertThat(db.level1Meter().get().levelSize(), equalTo(0L))
                    assertThat(db.levelMeter(1).get().levelSize(), equalTo(0L))
                    assertThat(db.levelMeter(8).isPresent, equalTo(false))
                }
    }

    @Test
    fun persistentSetIntMightContain() {
        swaydb.kotlin.eventually.persistent.Set
                .builder<Int>()
                .withDir(addTarget(Paths.get("disk4mightContain")))
                .withKeySerializer(Int::class)
                .build().use { db ->
                    db.add(1)
                    assertThat(db.mightContain(1), equalTo(true))
                }
    }

    @Test
    fun persistentSetIntAsJava() {
        swaydb.kotlin.eventually.persistent.Set
                .builder<Int>()
                .withDir(addTarget(Paths.get("disk4asJava")))
                .withKeySerializer(Int::class)
                .build().use { db ->
                    db.add(1)
                    assertThat(db.asJava().size, equalTo(1))
                }
    }

    @Test
    fun persistentSetIntClear() {
        swaydb.kotlin.eventually.persistent.Set
                .builder<Int>()
                .withDir(addTarget(Paths.get("disk4clear")))
                .withKeySerializer(Int::class)
                .build().use { db ->
                    db.add(1)
                    assertThat(db.isEmpty, equalTo(false))
                    db.clear()
                    assertThat(db.isEmpty, equalTo(true))
                }
    }

    @Suppress("UNCHECKED_CAST")
    @Test
    fun persistentSetIntFromBuilder() {
        // val db = eventually.persistent.Set[Int](dir = dir.resolve("disk4builder")).get
        val db = swaydb.kotlin.eventually.persistent.Set
                .builder<Int>()
                .withDir(addTarget(Paths.get("disk4builder")))
                .withKeySerializer(Int::class)
                .withMaxOpenSegments(swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$2`<Any, Any>())
                .withMapSize(swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$3`<Any, Any>())
                .withMaxMemoryLevelSize(swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$4`<Any, Any>())
                .withMaxSegmentsToPush(swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$5`<Any, Any>())
                .withMemoryLevelSegmentSize(swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$6`<Any, Any>())
                .withPersistentLevelSegmentSize(swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$7`<Any, Any>())
                .withPersistentLevelAppendixFlushCheckpointSize(
                        swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$8`<Any, Any>())
                .withMmapPersistentSegments(swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$9`<Any, Any>())
                .withMmapPersistentAppendix(swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$10`<Any, Any>())
                .withCacheSize(swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$11`<Any, Any>())
                .withOtherDirs(swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$12`<Any, Any>())
                .withCacheCheckDelay(swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$13`<Any, Any>())
                .withSegmentsOpenCheckDelay(swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$14`<Any, Any>())
                .withBloomFilterFalsePositiveRate(swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$15`<Any, Any>())
                .withCompressDuplicateValues(swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$16`<Any, Any>())
                .withDeleteSegmentsEventually(swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$17`<Any, Any>())
                .withGroupingStrategy(swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$18`<Any, Any>())
                .withAcceleration(swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$19`<Any, Any>())
                .withAcceleration(swaydb.persistent.`Map$`.`MODULE$`.`apply$default$18`<Any, Any>())
                .build()
        // db.add(1).get
        db.add(1)
        // db.contains(1).get
        val result = db.contains(1)
        assertThat("result contains value", result, equalTo(true))
        // db.remove(1).get
        db.remove(1)
        val result2 = db.contains(1)
        assertThat("Empty result", result2, equalTo(false))
    }
}
