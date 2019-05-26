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
import scala.runtime.`Nothing$`
import swaydb.Prepare
import swaydb.base.TestBase
import java.time.Duration

class QuickStartMemorySetTest : TestBase() {

    @Test
    fun memorySetInt() {
        // Create a memory set database.
        // val db = memory.Set[Int].get

        val db = swaydb.kotlin.memory.Set.create<Int>(
                Int::class)
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
    fun memorySetIntIterator() {
        val db = swaydb.kotlin.memory.Set
                .builder<Int>()
                .withKeySerializer(Int::class)
                .build()
        db.add(1)
        assertThat(db.iterator().next(), equalTo(1))
    }

    @Test
    fun memorySetIntToArray() {
        val db = swaydb.kotlin.memory.Set
                .builder<Int>()
                .withKeySerializer(Int::class)
                .build()
        db.add(1)
        assertThat(Arrays.toString(db.toArray()), equalTo("[1]"))
    }

    @Test
    fun memorySetIntAddExpireAfter() {
        swaydb.kotlin.memory.Set
                .builder<Int>()
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
    fun memorySetIntAddExpireAt() {
        swaydb.kotlin.memory.Set
                .builder<Int>()
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
    fun memorySetIntExpireAfter() {
        swaydb.kotlin.memory.Set
                .builder<Int>()
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
    fun memorySetIntExpireAt() {
        swaydb.kotlin.memory.Set
                .builder<Int>()
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
    fun memorySetIntContainsAll() {
        swaydb.kotlin.memory.Set
                .builder<Int>()
                .withKeySerializer(Int::class)
                .build().use { db ->
                    db.add(1)
                    assertThat(db.containsAll(Arrays.asList(1)), equalTo(true))
                }
    }

    @Test
    fun memorySetIntAddAll() {
        swaydb.kotlin.memory.Set
                .builder<Int>()
                .withKeySerializer(Int::class)
                .build().use { db ->
                    db.add(1)
                    db.add(Arrays.asList(2))
                    assertThat(db.containsAll(Arrays.asList(1, 2)), equalTo(true))
                }
    }

    @Test
    fun memorySetIntRetainAll() {
        swaydb.kotlin.memory.Set
                .builder<Int>()
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
    fun memorySetIntRetainAll2() {
        swaydb.kotlin.memory.Set
                .builder<String>()
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
    fun memorySetIntRemove() {
        swaydb.kotlin.memory.Set
                .builder<Int>()
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
    fun memorySetIntSize() {
        swaydb.kotlin.memory.Set
                .builder<Int>()
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
    fun memorySetIntIsEmpty() {
        swaydb.kotlin.memory.Set
                .builder<Int>()
                .withKeySerializer(Int::class)
                .build().use { db ->
                    assertThat(db.isEmpty, equalTo(true))
                    db.add(1)
                    assertThat(db.isEmpty, equalTo(false))
                }
    }

    @Test
    fun memorySetIntNonEmpty() {
        swaydb.kotlin.memory.Set
                .builder<Int>()
                .withKeySerializer(Int::class)
                .build().use { db ->
                    assertThat(db.nonEmpty(), equalTo(false))
                    db.add(1)
                    assertThat(db.nonEmpty(), equalTo(true))
                }
    }

    @Test
    fun memorySetIntExpiration() {
        swaydb.kotlin.memory.Set
                .builder<Int>()
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
    fun memorySetIntTimeLeft() {
        swaydb.kotlin.memory.Set
                .builder<Int>()
                .withKeySerializer(Int::class)
                .build().use { db ->
                    val expireAt = LocalDateTime.now().plusNanos(TimeUnit.MILLISECONDS.toNanos(100))
                    db.add(1, expireAt)
                    assertThat(db.timeLeft(1)!!.seconds, equalTo(0L))
                    assertThat<Duration>(db.timeLeft(2), nullValue())
                }
    }

    @Test
    fun memorySetIntSizes() {
        swaydb.kotlin.memory.Set
                .builder<Int>()
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
    fun memorySetIntMightContain() {
        swaydb.kotlin.memory.Set
                .builder<Int>()
                .withKeySerializer(Int::class)
                .build().use { db ->
                    db.add(1)
                    assertThat(db.mightContain(1), equalTo(true))
                }
    }

    @Test
    fun memorySetIntAsJava() {
        swaydb.kotlin.memory.Set
                .builder<Int>()
                .withKeySerializer(Int::class)
                .build().use { db ->
                    db.add(1)
                    assertThat(db.asJava()!!.size, equalTo(1))
                }
    }

    @Test
    fun memorySetIntClear() {
        swaydb.kotlin.memory.Set
                .builder<Int>()
                .withKeySerializer(Int::class)
                .build().use { db ->
                    db.add(1)
                    assertThat(db.isEmpty, equalTo(false))
                    db.clear()
                    assertThat(db.isEmpty, equalTo(true))
                }
    }

    @Test
    fun persistentSetIntFromBuilder() {
        // val db = memory.Set[Int].get
        val db = swaydb.kotlin.memory.Set
                .builder<Int>()
                .withKeySerializer(Int::class)
                .withMapSize(4000000)
                .withSegmentSize(2000000)
                .withCacheSize(100000000)
                .withCacheCheckDelay(scala.concurrent.duration.FiniteDuration.apply(5, TimeUnit.SECONDS))
                .withBloomFilterFalsePositiveRate(0.01)
                .withCompressDuplicateValues(true)
                .withDeleteSegmentsEventually(false)
                .withGroupingStrategy(scala.Option.empty())
                .withAcceleration(swaydb.memory.`Map$`.`MODULE$`.`apply$default$9`<Any, Any>())
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
