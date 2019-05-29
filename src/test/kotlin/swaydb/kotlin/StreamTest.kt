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
package swaydb.kotlin

import org.junit.Test
import scala.collection.mutable.ListBuffer
import swaydb.data.IO

class StreamTest {

    @Test(expected = TypeCastException::class)
    fun map() {
        Stream(null as swaydb.Stream<Int, String>?).map { entry -> entry }
    }

    @Test(expected = TypeCastException::class)
    fun materialize() {
        Stream(null as swaydb.Stream<Int, String>?).materialize()
    }

    @Test
    fun foreach() {
        Stream(null as swaydb.Stream<Int, String>?).foreach {entry -> println(entry)}
    }

}
