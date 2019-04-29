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

import org.awaitility.Awaitility.await
import org.hamcrest.CoreMatchers.*
import org.junit.Assert.assertThat
import org.junit.Test
import swaydb.Prepare
import swaydb.data.api.grouping.KeyValueGroupingStrategy
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import java.util.*
import java.util.concurrent.TimeUnit

class QuickStartMemoryMapTest {

    @Suppress("UNCHECKED_CAST")
    @Test
    fun memoryMapIntStringFrom() {
        // Create a memory database
        // val db = memory.Map[Int, String]().get
        swaydb.memory.Map.create<Int, String>(
                Int::class, String::class).use { db ->
                    // db.put(1, "one").get
                    db.put(1, "one")
                    // db.get(1).get
                    val result = db.get(1)
                    assertThat("result contains value", result, notNullValue())
                    assertThat("Key 1 is present", db.containsKey(1), equalTo(true))
                    assertThat(result, equalTo("one"))
                    // db.remove(1).get
                    db.remove(1)
                    val result2 = db.get(1)
                    assertThat("Empty result", result2, nullValue())
                    // db.put(1, "one value").get
                    db.put(1, "one value")

            db.commit(
                    swaydb.kotlin.Prepare.put(2, "two value"),
                    swaydb.kotlin.Prepare.remove(1) as Prepare<Int, String>
            )
            assertThat(db.get(2), equalTo("two value"));
            assertThat(db.get(1), nullValue());

            // write 100 key-values atomically
            db.put((1..100)
                    .map { index -> AbstractMap.SimpleEntry<Int, String>(index, index.toString()) }
                    .map { it.key to it.value + "_updated" }
                    .toMap().toMutableMap())
            // assert the key-values were updated
            (1..100)
                    .map { item -> AbstractMap.SimpleEntry<Int, String>(item, db.get(item)) }
                    .forEach { pair -> assertThat(pair.value.endsWith("_updated"), equalTo(true)) }
        }
    }

    @Test
    fun memoryMapIntStringClear() {
        // Create a memory database
        swaydb.memory.Map
                .builder<Int, String>()
                .withKeySerializer(Int::class)
                .withValueSerializer(String::class)
                .build().use { db ->
                    // db.put(1, "one").get
                    db.put(1, "one")
                    // db.get(1).get
                    val result = db.get(1)
                    assertThat<String>("result contains value", result, notNullValue())
                    assertThat("Key 1 is present", db.containsKey(1), equalTo(true))
                    assertThat<String>(result, equalTo("one"))
                    db.clear()
                    val result2 = db.get(1)
                    assertThat<String>("Empty result", result2, nullValue())
                }
    }

    @Test
    fun memoryMapIntStringSize() {
        swaydb.memory.Map
                .builder<Int, String>()
                .withKeySerializer(Int::class)
                .withValueSerializer(String::class)
                .build().use { db ->
                    assertThat(db.size(), equalTo(0))
                    db.put(1, "one")
                    assertThat(db.size(), equalTo(1))
                    db.remove(1)
                    assertThat(db.size(), equalTo(0))
                }
    }

    @Test
    fun memoryMapIntStringIsEmpty() {
        swaydb.memory.Map
                .builder<Int, String>()
                .withKeySerializer(Int::class)
                .withValueSerializer(String::class)
                .build().use { db ->
                    assertThat(db.isEmpty(), equalTo(true))
                    assertThat(db.nonEmpty(), equalTo(false))
                }
    }

    @Test
    fun memoryMapIntStringContainsValue() {
        swaydb.memory.Map
                .builder<Int, String>()
                .withKeySerializer(Int::class)
                .withValueSerializer(String::class)
                .build().use { db ->
                    db.put(1, "one")
                    assertThat(db.containsValue("one"), equalTo(true))
                }
    }

    @Test
    fun memoryMapIntStringMightContain() {
        swaydb.memory.Map
                .builder<Int, String>()
                .withKeySerializer(Int::class)
                .withValueSerializer(String::class)
                .build().use { db ->
                    db.put(1, "one")
                    assertThat(db.mightContain(1), equalTo(true))
                }
    }

    @Test
    fun memoryMapIntStringisHead() {
        swaydb.memory.Map
                .builder<Int, String>()
                .withKeySerializer(Int::class)
                .withValueSerializer(String::class)
                .build().use { db ->
                    db.put(1, "one")
                    assertThat(db.head().toString(), equalTo("1=one"))
                    assertThat(db.headOption().toString(), equalTo("Optional[1=one]"))
                    db.remove(1)
                    assertThat(db.head(), nullValue())
                }
    }

    @Test
    fun memoryMapIntStringisKeysHead() {
        swaydb.memory.Map
                .builder<Int, String>()
                .withKeySerializer(Int::class)
                .withValueSerializer(String::class)
                .build().use { db ->
                    db.put(1, "one")
                    assertThat(db.keysHead().toString(), equalTo("1"))
                    assertThat(db.keysHeadOption().toString(), equalTo("Optional[1]"))
                    db.remove(1)
                    assertThat(db.keysHead(), nullValue())
                }
    }

    @Test
    fun memoryMapIntStringisKeysLast() {
        swaydb.memory.Map
                .builder<Int, String>()
                .withKeySerializer(Int::class)
                .withValueSerializer(String::class)
                .build().use { db ->
                    db.put(1, "one")
                    db.put(2, "two")
                    assertThat(db.keysLast().toString(), equalTo("2"))
                    assertThat(db.keysLastOption().toString(), equalTo("Optional[2]"))
                    db.clear()
                    assertThat(db.keysLast(), nullValue())
                }
    }

    @Test
    fun memoryMapIntStringisLast() {
        swaydb.memory.Map
                .builder<Int, String>()
                .withKeySerializer(Int::class)
                .withValueSerializer(String::class)
                .build().use { db ->
                    db.put(1, "one")
                    db.put(2, "two")
                    assertThat(db.last().toString(), equalTo("2=two"))
                    assertThat(db.lastOption().toString(), equalTo("Optional[2=two]"))
                    db.clear()
                    assertThat(db.last(), nullValue())
                }
    }

    @Test
    fun memoryMapIntStringPutMap() {
        swaydb.memory.Map
                .builder<Int, String>()
                .withKeySerializer(Int::class)
                .withValueSerializer(String::class)
                .build().use { db ->
                    val data = LinkedHashMap<Int, String>()
                    data[1] = "one"
                    db.put(data)
                    assertThat(db.containsValue("one"), equalTo(true))
                }
    }

    @Test
    fun memoryMapIntStringUpdateMap() {
        swaydb.memory.Map
                .builder<Int, String>()
                .withKeySerializer(Int::class)
                .withValueSerializer(String::class)
                .build().use { db ->
                    db.put(1, "zerro")
                    val data = LinkedHashMap<Int, String>()
                    data[1] = "one"
                    db.update(data)
                    assertThat(db.containsValue("one"), equalTo(true))
                }
    }

    @Test
    fun memoryMapIntStringisKeySet() {
        swaydb.memory.Map
                .builder<Int, String>()
                .withKeySerializer(Int::class)
                .withValueSerializer(String::class)
                .build().use { db ->
                    db.put(1, "one")
                    assertThat(db.keySet().toString(), equalTo("[1]"))
                }
    }

    @Test
    fun memoryMapIntStringisValues() {
        swaydb.memory.Map
                .builder<Int, String>()
                .withKeySerializer(Int::class)
                .withValueSerializer(String::class)
                .build().use { db ->
                    db.put(1, "one")
                    assertThat(db.values().toString(), equalTo("[one]"))
                }
    }

    @Test
    fun memoryMapIntStringisEntrySet() {
        swaydb.memory.Map
                .builder<Int, String>()
                .withKeySerializer(Int::class)
                .withValueSerializer(String::class)
                .build().use { db ->
                    db.put(1, "one")
                    assertThat(db.entrySet().toString(), equalTo("[1=one]"))
                }
    }

    @Test
    fun memoryMapIntStringPutExpireAfter() {
        swaydb.memory.Map
                .builder<Int, String>()
                .withKeySerializer(Int::class)
                .withValueSerializer(String::class)
                .build().use { db ->
                    db.put(1, "one", 100, TimeUnit.MILLISECONDS)
                    assertThat(db.entrySet().toString(), equalTo("[1=one]"))
                    await().atMost(1600, TimeUnit.MILLISECONDS).until {
                        assertThat(db.get(1), nullValue())
                        true
                    }
                }
    }

    @Test
    fun memoryMapIntStringPutExpireAt() {
        swaydb.memory.Map
                .builder<Int, String>()
                .withKeySerializer(Int::class)
                .withValueSerializer(String::class)
                .build().use { db ->
                    db.put(1, "one", LocalDateTime.now().plusNanos(TimeUnit.MILLISECONDS.toNanos(100)))
                    assertThat(db.entrySet().toString(), equalTo("[1=one]"))
                    await().atMost(1200, TimeUnit.MILLISECONDS).until {
                        assertThat(db.get(1), nullValue())
                        true
                    }
                }
    }

    @Test
    fun memoryMapIntStringExpiration() {
        swaydb.memory.Map
                .builder<Int, String>()
                .withKeySerializer(Int::class)
                .withValueSerializer(String::class)
                .build().use { db ->
                    val expireAt = LocalDateTime.now().plusNanos(TimeUnit.MILLISECONDS.toNanos(100))
                    db.put(1, "one", expireAt)
                    assertThat(db.expiration(1)?.truncatedTo(ChronoUnit.SECONDS).toString(),
                            equalTo(expireAt.truncatedTo(ChronoUnit.SECONDS).toString()))
                    assertThat(db.expiration(2), nullValue())
                }
    }

    @Test
    fun memoryMapIntStringTimeLeft() {
        swaydb.memory.Map
                .builder<Int, String>()
                .withKeySerializer(Int::class)
                .withValueSerializer(String::class)
                .build().use { db ->
                    val expireAt = LocalDateTime.now().plusNanos(TimeUnit.MILLISECONDS.toNanos(100))
                    db.put(1, "one", expireAt)
                    assertThat(db.timeLeft(1)?.seconds, equalTo(0L))
                    assertThat(db.timeLeft(2), nullValue())
                }
    }

    @Test
    fun memoryMapIntStringKeySize() {
        swaydb.memory.Map
                .builder<Int, String>()
                .withKeySerializer(Int::class)
                .withValueSerializer(String::class)
                .build().use { db ->
                    db.put(1, "one")
                    assertThat(db.keySize(1), equalTo(4))
                }
    }

    @Test
    fun memoryMapIntStringValueSize() {
        swaydb.memory.Map
                .builder<Int, String>()
                .withKeySerializer(Int::class)
                .withValueSerializer(String::class)
                .build().use { db ->
                    db.put(1, "one")
                    assertThat(db.valueSize("one"), equalTo(3))
                }
    }

    @Test
    fun memoryMapIntStringSizes() {
        swaydb.memory.Map
                .builder<Int, String>()
                .withKeySerializer(Int::class)
                .withValueSerializer(String::class)
                .build().use { db ->
                    db.put(1, "one")
                    assertThat(db.sizeOfSegments(), equalTo(0L))
                    assertThat(db.level0Meter().currentMapSize(), equalTo(4000000L))
                    assertThat(db.level1Meter().get().levelSize(), equalTo(0L))
                    assertThat(db.levelMeter(1).get().levelSize(), equalTo(0L))
                    assertThat(db.levelMeter(8).isPresent, equalTo(false))
                }
    }

    @Test
    fun memoryMapIntStringExpireAfter() {
        swaydb.memory.Map
                .builder<Int, String>()
                .withKeySerializer(Int::class)
                .withValueSerializer(String::class)
                .build().use { db ->
                    db.put(1, "one")
                    db.expire(1, 100, TimeUnit.MILLISECONDS)
                    assertThat(db.entrySet().toString(), equalTo("[1=one]"))
                    await().atMost(1800, TimeUnit.MILLISECONDS).until {
                        assertThat(db.get(1), nullValue())
                        true
                    }
                }
    }

    @Test
    fun memoryMapIntStringExpireAt() {
        swaydb.memory.Map
                .builder<Int, String>()
                .withKeySerializer(Int::class)
                .withValueSerializer(String::class)
                .build().use { db ->
                    db.put(1, "one")
                    db.expire(1, LocalDateTime.now().plusNanos(TimeUnit.MILLISECONDS.toNanos(100)))
                    assertThat(db.entrySet().toString(), equalTo("[1=one]"))
                    await().atMost(1800, TimeUnit.MILLISECONDS).until {
                        assertThat(db.get(1), nullValue())
                        true
                    }
                }
    }

    @Test
    fun memoryMapIntStringUpdate() {
        swaydb.memory.Map
                .builder<Int, String>()
                .withKeySerializer(Int::class)
                .withValueSerializer(String::class)
                .build().use { db ->
                    db.put(1, "one")
                    db.update(1, "one+1")
                    assertThat(db.get(1), equalTo<String>("one+1"))
                }
    }

    @Test
    fun memoryMapIntStringAsJava() {
        swaydb.memory.Map
                .builder<Int, String>()
                .withKeySerializer(Int::class)
                .withValueSerializer(String::class)
                .build().use { db->
                    db.put(1, "one")
                    assertThat(db.asJava()?.size, equalTo(1)) }
    }

    @Test
    fun memoryMapIntStringRemove() {
        swaydb.memory.Map
                .builder<Int, String>()
                .withKeySerializer(Int::class)
                .withValueSerializer(String::class)
                .build().use { db ->
                    db.put(1, "one")
                    db.put(2, "two")
                    db.remove(1, 2)
                    assertThat(db.size(), equalTo(0))
                    db.put(3, "three")
                    db.put(4, "four")
                    db.remove(HashSet(Arrays.asList(3, 4)))
                    assertThat(db.size(), equalTo(0))
                }
    }

    @Test
    fun memoryMapIntStringFromBuilder() {
        swaydb.memory.Map
                .builder<Int, String>()
                .withKeySerializer(Int::class)
                .withValueSerializer(String::class)
                .withMapSize(4000000)
                .withSegmentSize(2000000)
                .withCacheSize(100000000)
                .withCacheCheckDelay(scala.concurrent.duration.FiniteDuration.apply(5, TimeUnit.SECONDS))
                .withBloomFilterFalsePositiveRate(0.01)
                .withCompressDuplicateValues(true)
                .withDeleteSegmentsEventually(false)
                .withGroupingStrategy(scala.Option.empty<KeyValueGroupingStrategy>())
                .withAcceleration(swaydb.memory.`Map$`.`MODULE$`.`apply$default$9`<Any, Any>())
                .build().use { db ->
                    // db.put(1, "one").get
                    db.put(1, "one")
                    // db.get(1).get
                    val result = db.get(1)
                    assertThat<String>("result contains value", result, notNullValue())
                    assertThat("Key 1 is present", db.containsKey(1), equalTo(true))
                    assertThat<String>(result, equalTo("one"))
                    // db.remove(1).get
                    db.remove(1)
                    val result2 = db.get(1)
                    assertThat<String>("Empty result", result2, nullValue())
                }
    }
}
