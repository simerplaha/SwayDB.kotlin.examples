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
import org.junit.BeforeClass
import org.junit.Test
import scala.collection.Seq
import scala.collection.mutable.ListBuffer
import scala.runtime.AbstractFunction1
import swaydb.Prepare
import swaydb.base.TestBase
import swaydb.data.IO
import swaydb.data.config.Dir
import swaydb.data.config.MMAP
import swaydb.data.config.RecoveryMode
import swaydb.data.slice.Slice
import swaydb.kotlin.ApacheSerializer
import swaydb.kotlin.Apply
import java.io.IOException
import java.nio.file.Paths
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import java.util.*
import java.util.AbstractMap.SimpleEntry
import java.util.concurrent.TimeUnit
import java.util.stream.IntStream

class QuickStartEventuallyPersistentMapTest : TestBase() {

    companion object {
        @BeforeClass
        @JvmStatic
        @Throws(IOException::class)
        fun beforeClass() {
            TestBase().deleteDirectoryWalkTreeStartsWith("target/disk2")
        }
    }

    @Suppress("UNCHECKED_CAST")
    @Test
    fun persistentMapIntStringFrom() {
        // Create a persistent database
        // val db = eventually.persistent.Map[Int, String]().get
        swaydb.kotlin.eventually.persistent.Map.create<Int, String>(
                Int::class, String::class, addTarget(Paths.get("disk2From"))).use({ db ->
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

            @Suppress("UNCHECKED_CAST")
            db.commit(
                    swaydb.kotlin.Prepare.put(2, "two value"),
                    swaydb.kotlin.Prepare.put(3, "three value", 1000, TimeUnit.MILLISECONDS),
                    swaydb.kotlin.Prepare.remove(1) as Prepare<Int, String>
            )

            assertThat(db.get(2), equalTo("two value"))
            assertThat(db.get(1), nullValue())

            // write 100 key-values atomically
            db.put((1..100)
                    .map { index -> SimpleEntry<Int, String>(index, index.toString()) }
                    .map { it.key to it.value }
                    .toMap().toMutableMap())

            // Iteration: fetch all key-values withing range 10 to 90, update values
            // and atomically write updated key-values
            db
                    .from(10)
                    .takeWhile({ item: MutableMap.MutableEntry<Int, String> -> item.key <= 90 })
                    .map({ item -> SimpleEntry(item.key, item.value + "_updated") })
                    .materialize()
                    .foreach({entry -> db.put(entry)})
            // assert the key-values were updated
            (10..90)
                    .map { item -> SimpleEntry<Int, String>(item, db.get(item)) }
                    .forEach { pair -> assertThat(pair.value.endsWith("_updated"), equalTo(true)) }
        })
    }

    @Test
    fun persistentMapIntStringFromOrAfter() {
        swaydb.kotlin.eventually.persistent.Map.create<Int, String>(
                Int::class, String::class, addTarget(Paths.get("disk2FromOrAfter"))).use({ db ->
            // write 100 key-values atomically
            db.put((1..100)
                    .map { index -> SimpleEntry<Int, String>(index, index.toString()) }
                    .map { it.key to it.value }
                    .toMap().toMutableMap())
            db
                    .fromOrAfter(10)
                    .takeWhile({ item: MutableMap.MutableEntry<Int, String> -> item.key <= 90 })
                    .map({ item -> SimpleEntry(item.key, item.value + "_updated") })
                    .materialize()
                    .foreach({entry -> db.put(entry)})
            // assert the key-values were updated
            (10..90)
                    .map { item -> SimpleEntry<Int, String>(item, db.get(item)) }
                    .forEach { pair -> assertThat(pair.value.endsWith("_updated"), equalTo(true)) }
        })
    }

    @Test
    fun persistentMapIntStringFromOrBefore() {
        swaydb.kotlin.eventually.persistent.Map.create<Int, String>(
                Int::class, String::class, addTarget(Paths.get("disk2FromOrBefore"))).use({ db ->
            // write 100 key-values atomically
            db.put((1..100)
                    .map { index -> SimpleEntry<Int, String>(index, index.toString()) }
                    .map { it.key to it.value }
                    .toMap().toMutableMap())
            db
                    .fromOrBefore(10)
                    .takeWhile({ item: MutableMap.MutableEntry<Int, String> -> item.key <= 90 })
                    .map({ item -> SimpleEntry(item.key, item.value + "_updated") })
                    .materialize()
                    .foreach({entry -> db.put(entry)})
            // assert the key-values were updated
            (10..90)
                    .map { item -> SimpleEntry<Int, String>(item, db.get(item)) }
                    .forEach { pair -> assertThat(pair.value.endsWith("_updated"), equalTo(true)) }
        })
    }

    @Suppress("UNCHECKED_CAST")
    @Test
    fun persistentMapIntStringKeys() {
        swaydb.kotlin.eventually.persistent.Map.create<Int, String>(
                Int::class, String::class, addTarget(Paths.get("disk2Keys"))).use({ db ->
            // write 100 key-values atomically
            db.put((1..100)
                    .map { index -> SimpleEntry<Int, String>(index, index.toString()) }
                    .map { it.key to it.value }
                    .toMap().toMutableMap())

            val result = LinkedHashSet<Int>()
            (db
                    .keys()
                    .reverse()
                    .fromOrBefore(10)
                    .take(5)
                    .materialize() as IO.Success<ListBuffer<*>>)
                    .foreach(object : AbstractFunction1<ListBuffer<*>, Any?>() {
                        override fun apply(t1: ListBuffer<*>): Any? {
                            val entries = t1.seq()
                            var index = 0
                            while (index < entries.size()) {
                                result.add(entries.apply(index) as Int)
                                index += 1
                            }
                            return null
                        }
                    })
            assertThat(result.toString(), equalTo("[10, 9, 8, 7, 6]"))
        })
    }

    @Suppress("UNCHECKED_CAST")
    @Test
    fun persistentMapIntStringReverse() {
        swaydb.kotlin.eventually.persistent.Map.create<Int, String>(
                Int::class, String::class, addTarget(Paths.get("disk2Reverse"))).use({ db ->
            // write 10 key-values atomically
            db.put((1..10)
                    .map { index -> SimpleEntry<Int, String>(index, index.toString()) }
                    .map { it.key to it.value }
                    .toMap().toMutableMap())

            val result = LinkedHashSet<Int>()
            (db
                    .reverse()
                    .keys()
                    .fromOrBefore(10)
                    .take(5)
                    .materialize() as IO.Success<ListBuffer<*>>)
                    .foreach(object : AbstractFunction1<ListBuffer<*>, Any?>() {
                        override fun apply(t1: ListBuffer<*>): Any? {
                            val entries = t1.seq()
                            var index = 0
                            while (index < entries.size()) {
                                result.add(entries.apply(index) as Int)
                                index += 1
                            }
                            return null
                        }
                    })
            assertThat(result.toString(), equalTo("[10, 9, 8, 7, 6]"))
        })
    }

    @Test
    fun persistentMapIntStringMap() {
        swaydb.kotlin.eventually.persistent.Map.create<Int, String>(
                Int::class, String::class, addTarget(Paths.get("disk2Map"))).use({ db ->
            // write 10 key-values atomically
            db.put((1..10)
                    .map { index -> SimpleEntry<Int, String>(index, index.toString()) }
                    .map { it.key to it.value }
                    .toMap().toMutableMap())

            val result = LinkedHashSet<MutableMap.MutableEntry<Int, String>>()
            db
                    .map{ item -> SimpleEntry(item.key, item.value + "_updated") }
                    .materialize()
                    .foreach({entry -> result.add(entry)})
            assertThat(result.toString(), equalTo("[1=1_updated, 2=2_updated, 3=3_updated,"
                    + " 4=4_updated, 5=5_updated, 6=6_updated, 7=7_updated, 8=8_updated,"
                    + " 9=9_updated, 10=10_updated]"))
        })
    }

    @Test
    fun persistentMapIntStringDrop() {
        swaydb.kotlin.eventually.persistent.Map.create<Int, String>(
                Int::class, String::class, addTarget(Paths.get("disk2Drop"))).use({ db ->
            // write 10 key-values atomically
            db.put((1..10)
                    .map { index -> SimpleEntry<Int, String>(index, index.toString()) }
                    .map { it.key to it.value }
                    .toMap().toMutableMap())

            val result = LinkedHashSet<MutableMap.MutableEntry<Int, String>>()
            db
                    .drop(5)
                    .materialize()
                    .foreach({entry -> result.add(entry)})
            assertThat(result.toString(), equalTo("[6=6, 7=7, 8=8, 9=9, 10=10]"))
        })
    }

    @Test
    fun persistentMapIntStringDropWhile() {
        swaydb.kotlin.eventually.persistent.Map.create<Int, String>(
                Int::class, String::class, addTarget(Paths.get("disk2DropWhile"))).use({ db ->
            // write 100 key-values atomically
            db.put((1..100)
                    .map { index -> SimpleEntry<Int, String>(index, index.toString()) }
                    .map { it.key to it.value }
                    .toMap().toMutableMap())

            db
                    .from(1)
                    .dropWhile({ item -> item.key < 10 })
                    .map({ item -> SimpleEntry(item.key, item.value + "_updated") })
                    .materialize()
                    .foreach({entry -> db.put(entry)})
            // assert the key-values were updated
            IntStream.rangeClosed(10, 90)
                    .mapToObj<SimpleEntry<Int, String?>> { item -> SimpleEntry(item, db.get(item)) }
                    .forEach { pair -> assertThat(pair.value?.endsWith("_updated"), equalTo(true)) }
        })
    }

    @Test
    fun persistentMapIntStringTake() {
        swaydb.kotlin.eventually.persistent.Map.create<Int, String>(
                Int::class, String::class, addTarget(Paths.get("disk2Take"))).use({ db ->
            // write 10 key-values atomically
            db.put((1..10)
                    .map { index -> SimpleEntry<Int, String>(index, index.toString()) }
                    .map { it.key to it.value }
                    .toMap().toMutableMap())

            val result = LinkedHashSet<MutableMap.MutableEntry<Int, String>>()
            db
                    .take(5)
                    .materialize()
                    .foreach({entry -> result.add(entry)})
            assertThat(result.toString(), equalTo("[1=1, 2=2, 3=3, 4=4, 5=5]"))
        })
    }

    @Test
    fun persistentMapIntStringFilter() {
        swaydb.kotlin.eventually.persistent.Map.create<Int, String>(
                Int::class, String::class, addTarget(Paths.get("disk2Filter"))).use({ db ->
            // write 10 key-values atomically
            db.put((1..10)
                    .map { index -> SimpleEntry<Int, String>(index, index.toString()) }
                    .map { it.key to it.value }
                    .toMap().toMutableMap())

            val result = LinkedHashSet<MutableMap.MutableEntry<Int, String>>()
            db
                    .filter({ item -> item.key < 5 })
                    .materialize()
                    .foreach({entry -> result.add(entry)})
            assertThat(result.toString(), equalTo("[1=1, 2=2, 3=3, 4=4]"))
        })
    }

    @Test
    fun persistentMapIntStringForeach() {
        swaydb.kotlin.eventually.persistent.Map.create<Int, String>(
                Int::class, String::class, addTarget(Paths.get("disk2Foreach"))).use({ db ->
            // write 10 key-values atomically
            db.put((1..10)
                    .map { index -> SimpleEntry<Int, String>(index, index.toString()) }
                    .map { it.key to it.value }
                    .toMap().toMutableMap())

            val result = LinkedHashSet<MutableMap.MutableEntry<Int, String>>()
            db
                    .foreach({entry -> result.add(entry)})
                    .materialize()
            assertThat(result.toString(), equalTo("[1=1, 2=2, 3=3, 4=4, 5=5, 6=6, 7=7, 8=8, 9=9, 10=10]"))
        })
    }

    @Test
    fun persistentMapIntStringClear() {
        // Create a persistent database
        swaydb.kotlin.eventually.persistent.Map
                .builder<Int, String>()
                .withDir(addTarget(Paths.get("disk2builderClear")))
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
    fun persistentMapIntStringSize() {
        swaydb.kotlin.eventually.persistent.Map
                .builder<Int, String>()
                .withDir(addTarget(Paths.get("disk2builderSize")))
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
    fun persistentMapIntStringIsEmpty() {
        swaydb.kotlin.eventually.persistent.Map
                .builder<Int, String>()
                .withDir(addTarget(Paths.get("disk2builderIsEmpty")))
                .withKeySerializer(Int::class)
                .withValueSerializer(String::class)
                .build().use { db ->
                    assertThat(db.isEmpty(), equalTo(true))
                    assertThat(db.nonEmpty(), equalTo(false))
                }
    }

    @Test
    fun persistentMapIntStringContainsValue() {
        swaydb.kotlin.eventually.persistent.Map
                .builder<Int, String>()
                .withDir(addTarget(Paths.get("disk2builderContainsValue")))
                .withKeySerializer(Int::class)
                .withValueSerializer(String::class)
                .build().use { db ->
                    db.put(1, "one")
                    assertThat(db.containsValue("one"), equalTo(true))
                }
    }

    @Test
    fun persistentMapIntStringMightContain() {
        swaydb.kotlin.eventually.persistent.Map
                .builder<Int, String>()
                .withDir(addTarget(Paths.get("disk2builderMightContain")))
                .withKeySerializer(Int::class)
                .withValueSerializer(String::class)
                .build().use { db ->
                    db.put(1, "one")
                    assertThat(db.mightContain(1), equalTo(true))
                }
    }

    @Test
    fun persistentMapIntStringHead() {
        swaydb.kotlin.eventually.persistent.Map
                .builder<Int, String>()
                .withDir(addTarget(Paths.get("disk2builderHead")))
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
    fun persistentMapIntStringKeysHead() {
        swaydb.kotlin.eventually.persistent.Map
                .builder<Int, String>()
                .withDir(addTarget(Paths.get("disk2builderKeysHead")))
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
    fun persistentMapIntStringKeysLast() {
        swaydb.kotlin.eventually.persistent.Map
                .builder<Int, String>()
                .withDir(addTarget(Paths.get("disk2builderKeysLast")))
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
    fun persistentMapIntStringLast() {
        swaydb.kotlin.eventually.persistent.Map
                .builder<Int, String>()
                .withDir(addTarget(Paths.get("disk2builderLast")))
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
    fun persistentMapIntStringPutMap() {
        swaydb.kotlin.eventually.persistent.Map
                .builder<Int, String>()
                .withDir(addTarget(Paths.get("disk2builderPutMap")))
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
    fun persistentMapIntStringUpdateMap() {
        swaydb.kotlin.eventually.persistent.Map
                .builder<Int, String>()
                .withDir(addTarget(Paths.get("disk2builderUpdateMap")))
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
    fun persistentMapIntStringKeySet() {
        swaydb.kotlin.eventually.persistent.Map
                .builder<Int, String>()
                .withDir(addTarget(Paths.get("disk2builderKeySet")))
                .withKeySerializer(Int::class)
                .withValueSerializer(String::class)
                .build().use { db ->
                    db.put(1, "one")
                    assertThat(db.keySet().toString(), equalTo("[1]"))
                }
    }

    @Test
    fun persistentMapIntStringValues() {
        swaydb.kotlin.eventually.persistent.Map
                .builder<Int, String>()
                .withDir(addTarget(Paths.get("disk2builderValues")))
                .withKeySerializer(Int::class)
                .withValueSerializer(String::class)
                .build().use { db ->
                    db.put(1, "one")
                    assertThat(db.values().toString(), equalTo("[one]"))
                }
    }

    @Test
    fun persistentMapIntStringEntrySet() {
        swaydb.kotlin.eventually.persistent.Map
                .builder<Int, String>()
                .withDir(addTarget(Paths.get("disk2builderEntrySet")))
                .withKeySerializer(Int::class)
                .withValueSerializer(String::class)
                .build().use { db ->
                    db.put(1, "one")
                    assertThat(db.entrySet().toString(), equalTo("[1=one]"))
                }
    }

    @Test
    fun persistentMapIntStringPutExpireAfter() {
        swaydb.kotlin.eventually.persistent.Map
                .builder<Int, String>()
                .withDir(addTarget(Paths.get("disk2builderPutExpireAfter")))
                .withKeySerializer(Int::class)
                .withValueSerializer(String::class)
                .build().use { db ->
                    db.put(1, "one", 100, TimeUnit.MILLISECONDS)
                    assertThat(db.entrySet().toString(), equalTo("[1=one]"))
                    await().atMost(1800, TimeUnit.MILLISECONDS).until {
                        assertThat(db.get(1), nullValue())
                        true
                    }
                }
    }

    @Test
    fun persistentMapIntStringPutExpireAt() {
        swaydb.kotlin.eventually.persistent.Map
                .builder<Int, String>()
                .withDir(addTarget(Paths.get("disk2builderPutExpireAt")))
                .withKeySerializer(Int::class)
                .withValueSerializer(String::class)
                .build().use { db ->
                    db.put(1, "one", LocalDateTime.now().plusNanos(TimeUnit.MILLISECONDS.toNanos(100)))
                    assertThat(db.entrySet().toString(), equalTo("[1=one]"))
                    await().atMost(1800, TimeUnit.MILLISECONDS).until {
                        assertThat(db.get(1), nullValue())
                        true
                    }
                }
    }

    @Test
    fun persistentMapIntStringExpiration() {
        swaydb.kotlin.eventually.persistent.Map
                .builder<Int, String>()
                .withDir(addTarget(Paths.get("disk2builderExpiration")))
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
    fun persistentMapIntStringTimeLeft() {
        swaydb.kotlin.eventually.persistent.Map
                .builder<Int, String>()
                .withDir(addTarget(Paths.get("disk2builderTimeLeft")))
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
    fun persistentMapIntStringKeySize() {
        swaydb.kotlin.eventually.persistent.Map
                .builder<Int, String>()
                .withDir(addTarget(Paths.get("disk2builderKeySize")))
                .withKeySerializer(Int::class)
                .withValueSerializer(String::class)
                .build().use { db ->
                    db.put(1, "one")
                    assertThat(db.keySize(1), equalTo(4))
                }
    }

    @Test
    fun persistentMapIntStringValueSize() {
        swaydb.kotlin.eventually.persistent.Map
                .builder<Int, String>()
                .withDir(addTarget(Paths.get("disk2builderValueSize")))
                .withKeySerializer(Int::class)
                .withValueSerializer(String::class)
                .build().use { db ->
                    db.put(1, "one")
                    assertThat(db.valueSize("one"), equalTo(3))
                }
    }

    @Test
    fun persistentMapIntStringSizes() {
        swaydb.kotlin.eventually.persistent.Map
                .builder<Int, String>()
                .withDir(addTarget(Paths.get("disk2builderSizes")))
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
    fun persistentMapIntStringExpireAfter() {
        swaydb.kotlin.eventually.persistent.Map
                .builder<Int, String>()
                .withDir(addTarget(Paths.get("disk2builderExpireAfter")))
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
    fun persistentMapIntStringExpireAt() {
        swaydb.kotlin.eventually.persistent.Map
                .builder<Int, String>()
                .withDir(addTarget(Paths.get("disk2builderExpireAt")))
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
    fun persistentMapIntStringUpdate() {
        swaydb.kotlin.eventually.persistent.Map
                .builder<Int, String>()
                .withDir(addTarget(Paths.get("disk2builderUpdate")))
                .withKeySerializer(Int::class)
                .withValueSerializer(String::class)
                .build().use { db ->
                    db.put(1, "one")
                    db.update(1, "one+1")
                    assertThat(db.get(1), equalTo<String>("one+1"))
                }
    }

    @Test
    fun persistentMapIntStringAsJava() {
        swaydb.kotlin.eventually.persistent.Map
                .builder<Int, String>()
                .withDir(addTarget(Paths.get("disk2builderAsJava")))
                .withKeySerializer(Int::class)
                .withValueSerializer(String::class)
                .build().use { db ->
                    db.put(1, "one")
                    assertThat(db.asJava().size, equalTo(1))
                }
    }

    @Test
    fun persistentMapIntStringRemove() {
        swaydb.kotlin.eventually.persistent.Map
                .builder<Int, String>()
                .withDir(addTarget(Paths.get("disk2builderRemove")))
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
    fun persistentMapStringIntRegisterApplyFunctionUpdate() {
        swaydb.kotlin.eventually.persistent.Map
                .builder<String, Int>()
                .withDir(addTarget(Paths.get("disk2builderFunctionUpdate")))
                .withKeySerializer(String::class)
                .withValueSerializer(Int::class)
                .build().use { likesMap ->
                    // initial entry with 0 likes.
                    likesMap.put("SwayDB", 0)

                    val likesFunctionId = likesMap.registerFunction(
                            "increment likes counts") { likesCount: Int -> Apply.update(likesCount + 1) }
                    (1 .. 100).forEach { _ -> likesMap.applyFunction("SwayDB", likesFunctionId) }
                    assertThat(likesMap.get("SwayDB"), equalTo(100))
                }
    }

    @Test
    fun persistentMapStringIntRegisterApplyFunctionExpire() {
        swaydb.kotlin.eventually.persistent.Map
                .builder<String, Int>()
                .withDir(addTarget(Paths.get("disk2builderFunctionExpire")))
                .withKeySerializer(String::class)
                .withValueSerializer(Int::class)
                .build().use { likesMap ->
                    likesMap.put("SwayDB", 0)

                    val likesFunctionId = likesMap.registerFunction(
                            "expire likes counts") {
                        Apply.expire(
                                LocalDateTime.now().plusNanos(TimeUnit.MILLISECONDS.toNanos(100)))
                    }
                    likesMap.applyFunction("SwayDB", likesFunctionId)
                    assertThat(likesMap.get("SwayDB"), equalTo(0))
                    await().atMost(1200, TimeUnit.MILLISECONDS).until {
                        assertThat(likesMap.get("SwayDB"), nullValue())
                        true
                    }
                }
    }

    @Test
    fun persistentMapStringIntRegisterApplyFunctionRemove() {
        swaydb.kotlin.eventually.persistent.Map
                .builder<String, Int>()
                .withDir(addTarget(Paths.get("disk2builderFunctionRemove")))
                .withKeySerializer(String::class)
                .withValueSerializer(Int::class)
                .build().use { likesMap ->
                    likesMap.put("SwayDB", 0)

                    val likesFunctionId = likesMap.registerFunction(
                            "remove likes counts") { Apply.remove() }
                    likesMap.applyFunction("SwayDB", likesFunctionId)
                    assertThat<Int>(likesMap.get("SwayDB"), equalTo(null))
                }
    }

    @Test
    fun persistentMapStringIntRegisterApplyFunctionNothing() {
        swaydb.kotlin.eventually.persistent.Map
                .builder<String, Int>()
                .withDir(addTarget(Paths.get("disk2builderFunctionNothing")))
                .withKeySerializer(String::class)
                .withValueSerializer(Int::class)
                .build().use { likesMap ->
                    likesMap.put("SwayDB", 0)

                    val likesFunctionId = likesMap.registerFunction(
                            "nothing likes counts") { Apply.nothing() }
                    likesMap.applyFunction("SwayDB", likesFunctionId)
                    assertThat(likesMap.get("SwayDB"), equalTo(0))
                }
    }


    @Suppress("UNCHECKED_CAST")
    @Test
    fun persistentMapIntStringFromBuilder() {
        swaydb.kotlin.eventually.persistent.Map
                .builder<Int, String>()
                .withDir(addTarget(Paths.get("disk2builder")))
                .withKeySerializer(Int::class)
                .withValueSerializer(String::class)
                .withMaxOpenSegments(swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$2`<Int, String>())
                .withMapSize(swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$3`<Int, String>())
                .withMaxMemoryLevelSize(swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$4`<Int, String>())
                .withMaxSegmentsToPush(swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$5`<Int, String>())
                .withMemoryLevelSegmentSize(swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$6`<Int, String>())
                .withPersistentLevelSegmentSize(swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$7`<Int, String>())
                .withPersistentLevelAppendixFlushCheckpointSize(
                        swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$8`<Int, String>())
                .withMmapPersistentSegments(swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$9`<Int, String>())
                .withMmapPersistentAppendix(swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$10`<Int, String>())
                .withCacheSize(swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$11`<Int, String>())
                .withOtherDirs(swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$12`<Int, String>())
                .withCacheCheckDelay(swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$13`<Int, String>())
                .withSegmentsOpenCheckDelay(swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$14`<Int, String>())
                .withBloomFilterFalsePositiveRate(swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$15`<Int, String>())
                .withCompressDuplicateValues(swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$16`<Int, String>())
                .withDeleteSegmentsEventually(swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$17`<Int, String>())
                .withGroupingStrategy(swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$18`<Int, String>())
                .withAcceleration(swaydb.eventually.persistent.`Map$`.`MODULE$`.`apply$default$19`<Int, String>())
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

    internal class MyData1(key:String, value:String,
                          longValue:Long, byteValue:Byte, boolValue:Boolean) {
        var key:String
        var value:String
        var longValue:Long = 0
        var byteValue:Byte = 0
        var boolValue:Boolean = false
        init{
            this.key = key
            this.value = value
            this.longValue = longValue
            this.byteValue = byteValue
            this.boolValue = boolValue
        }
    }

    internal class MyDataSerializer:swaydb.serializers.Serializer<MyData1> {
        override fun write(data:MyData1): Slice<Any> {
            return swaydb.kotlin.Slice.create(4 + data.key.length + 4 + data.value.length + 10)
                    .addInt(data.key.length)
                    .addString(data.key)
                    .addInt(data.value.length)
                    .addString(data.value)
                    .addLong(data.longValue)
                    .addByte(data.byteValue)
                    .addBoolean(data.boolValue)
                    .close()
        }
        override fun read(data: Slice<Any>):MyData1 {
            val reader = swaydb.kotlin.BytesReader.create(data)
            val keyLength = reader.readInt()
            val key = reader.readString(keyLength)
            val valueLength = reader.readInt()
            val value = reader.readString(valueLength)
            val longValue = reader.readLong()
            val byteValue = reader.readByte()
            val boolValue = reader.readBoolean()
            return MyData1(key, value, longValue, byteValue, boolValue)
        }
    }

    @Test
    fun persistentMapIntCustom() {
        swaydb.kotlin.eventually.persistent.Map
                .builder<Int, MyData1>()
                .withDir(addTarget(Paths.get("disk2builderCustom")))
                .withKeySerializer(Int::class)
                .withValueSerializer(MyDataSerializer())
                .build().use({ db ->
                    // db.put(1, new MyData("one", "two")).get
                    val myData = MyData1("one", "two", 10L, 100.toByte(), true)
                    db.put(1, myData)
                    // db.get(1).get
                    val result = db.get(1)
                    assertThat("result contains value", result, notNullValue())
                    assertThat(result?.key, equalTo("one"))
                    assertThat(result?.value, equalTo("two"))
                    assertThat(result?.longValue, equalTo(10L))
                    assertThat(result?.byteValue, equalTo(100.toByte()))
                    assertThat(result?.boolValue, equalTo(true))
                    // db.remove(1).get
                    db.remove(1)
                    val result2 = db.get(1)
                    assertThat("Empty result", result2, nullValue())
                    val myData2 = MyData1("one", "two", 10L, 100.toByte(), false)
                    db.put(1, myData2)
                    val result3 = db.get(1)
                    assertThat(result3?.boolValue, equalTo(false))
                })
    }

    internal class MyData(var key: String, var value: String) : java.io.Serializable

    @Test
    fun persistentMapIntApacheSerializer() {

        swaydb.kotlin.eventually.persistent.Map
                .builder<Int, MyData>()
                .withDir(addTarget(Paths.get("disk2builderApache")))
                .withKeySerializer(Int::class)
                .withValueSerializer(ApacheSerializer<MyData>())
                .build().use({ db ->
                    // db.put(1, new MyData("one", "two")).get
                    val myData = MyData("one", "two")
                    db.put(1, myData)
                    // db.get(1).get
                    val result = db.get(1)
                    assertThat("result contains value", result, notNullValue())
                    assertThat(result?.key, equalTo("one"))
                    assertThat(result?.value, equalTo("two"))
                    // db.remove(1).get
                    db.remove(1)
                    val result2 = db.get(1)
                    assertThat("Empty result", result2, nullValue())
                })
    }

}
