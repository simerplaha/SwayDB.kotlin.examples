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

class QuickStartPersistentSetTest : TestBase() {

    companion object {
        @BeforeClass
        @JvmStatic
        @Throws(IOException::class)
        fun beforeClass() {
            TestBase().deleteDirectoryWalkTreeStartsWith("target/disk3")
        }
    }

    @Test
    fun persistentSetInt() {
        // Create a persistent set database.
        // val db = persistent.Set[Int].get

        val db = swaydb.kotlin.persistent.Set.create<Int>(
                Int::class, addTarget(Paths.get("disk3")))
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
        val db = swaydb.kotlin.persistent.Set
                .builder<Int>()
                .withDir(addTarget(Paths.get("disk3iterator")))
                .withKeySerializer(Int::class)
                .build()
        db.add(1)
        assertThat(db.iterator().next(), equalTo(1))
    }

    @Test
    fun persistentSetIntToArray() {
        val db = swaydb.kotlin.persistent.Set
                .builder<Int>()
                .withDir(addTarget(Paths.get("disk3toArray")))
                .withKeySerializer(Int::class)
                .build()
        db.add(1)
        assertThat(Arrays.toString(db.toArray()), equalTo("[1]"))
    }

    @Test
    fun persistentSetIntAddExpireAfter() {
        swaydb.kotlin.persistent.Set
                .builder<Int>()
                .withDir(addTarget(Paths.get("disk3addExpireAfter")))
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
        swaydb.kotlin.persistent.Set
                .builder<Int>()
                .withDir(addTarget(Paths.get("disk3addExpireAt")))
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
        swaydb.kotlin.persistent.Set
                .builder<Int>()
                .withDir(addTarget(Paths.get("disk3expireAfter")))
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
        swaydb.kotlin.persistent.Set
                .builder<Int>()
                .withDir(addTarget(Paths.get("disk3expireAt")))
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
        swaydb.kotlin.persistent.Set
                .builder<Int>()
                .withDir(addTarget(Paths.get("disk3containsAll")))
                .withKeySerializer(Int::class)
                .build().use { db ->
                    db.add(1)
                    assertThat(db.containsAll(Arrays.asList(1)), equalTo(true))
                }
    }

    @Test
    fun persistentSetIntAddAll() {
        swaydb.kotlin.persistent.Set
                .builder<Int>()
                .withDir(addTarget(Paths.get("disk3addAll")))
                .withKeySerializer(Int::class)
                .build().use { db ->
                    db.add(1)
                    db.add(Arrays.asList(2))
                    assertThat(db.containsAll(Arrays.asList(1, 2)), equalTo(true))
                }
    }

    @Test
    fun persistentSetIntRetainAll() {
        swaydb.kotlin.persistent.Set
                .builder<Int>()
                .withDir(addTarget(Paths.get("disk3retailAll")))
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
        swaydb.kotlin.persistent.Set
                .builder<String>()
                .withDir(addTarget(Paths.get("disk3retainAll2")))
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
        swaydb.kotlin.persistent.Set
                .builder<Int>()
                .withDir(addTarget(Paths.get("disk3remove")))
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
        swaydb.kotlin.persistent.Set
                .builder<Int>()
                .withDir(addTarget(Paths.get("disk3size")))
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
        swaydb.kotlin.persistent.Set
                .builder<Int>()
                .withDir(addTarget(Paths.get("disk3isEmpty")))
                .withKeySerializer(Int::class)
                .build().use { db ->
                    assertThat(db.isEmpty, equalTo(true))
                    db.add(1)
                    assertThat(db.isEmpty, equalTo(false))
                }
    }

    @Test
    fun persistentSetIntNonEmpty() {
        swaydb.kotlin.persistent.Set
                .builder<Int>()
                .withDir(addTarget(Paths.get("disk3nonEmpty")))
                .withKeySerializer(Int::class)
                .build().use { db ->
                    assertThat(db.nonEmpty(), equalTo(false))
                    db.add(1)
                    assertThat(db.nonEmpty(), equalTo(true))
                }
    }

    @Test
    fun persistentSetIntExpiration() {
        swaydb.kotlin.persistent.Set
                .builder<Int>()
                .withDir(addTarget(Paths.get("disk3expiration")))
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
        swaydb.kotlin.persistent.Set
                .builder<Int>()
                .withDir(addTarget(Paths.get("disk3timeLeft")))
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
        swaydb.kotlin.persistent.Set
                .builder<Int>()
                .withDir(addTarget(Paths.get("disk3sizes")))
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
        swaydb.kotlin.persistent.Set
                .builder<Int>()
                .withDir(addTarget(Paths.get("disk3mightContain")))
                .withKeySerializer(Int::class)
                .build().use { db ->
                    db.add(1)
                    assertThat(db.mightContain(1), equalTo(true))
                }
    }

    @Test
    fun persistentSetIntAsJava() {
        swaydb.kotlin.persistent.Set
                .builder<Int>()
                .withDir(addTarget(Paths.get("disk3asJava")))
                .withKeySerializer(Int::class)
                .build().use { db ->
                    db.add(1)
                    assertThat(db.asJava().size, equalTo(1))
                }
    }

    @Test
    fun persistentSetIntClear() {
        swaydb.kotlin.persistent.Set
                .builder<Int>()
                .withDir(addTarget(Paths.get("disk3clear")))
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
        // val db = persistent.Set[Int](dir = dir.resolve("disk3builder")).get
        val db = swaydb.kotlin.persistent.Set
                .builder<Int>()
                .withDir(addTarget(Paths.get("disk3builder")))
                .withKeySerializer(Int::class)
                .withMaxOpenSegments(1000)
                .withCacheSize(100000000)
                .withMapSize(4000000)
                .withMmapMaps(true)
                .withRecoveryMode(RecoveryMode.`ReportFailure$`.`MODULE$`)
                .withMmapAppendix(true)
                .withMmapSegments(MMAP.`WriteAndRead$`.`MODULE$`)
                .withSegmentSize(2000000)
                .withAppendixFlushCheckpointSize(2000000)
                .withOtherDirs(scala.collection.immutable.`Nil$`.`MODULE$` as Seq<Dir>)
                .withCacheCheckDelay(scala.concurrent.duration.FiniteDuration.apply(5, TimeUnit.SECONDS))
                .withSegmentsOpenCheckDelay(
                        scala.concurrent.duration.FiniteDuration.apply(5, TimeUnit.SECONDS))
                .withBloomFilterFalsePositiveRate(0.01)
                .withCompressDuplicateValues(true)
                .withDeleteSegmentsEventually(false)
                .withLastLevelGroupingStrategy(scala.Option.empty())
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
