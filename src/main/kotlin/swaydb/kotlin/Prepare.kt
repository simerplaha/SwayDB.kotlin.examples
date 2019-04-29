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

import scala.concurrent.duration.Deadline

object Prepare {

    fun <K, V> put(key: K, value: V): swaydb.Prepare.Put<K, V> {
        return swaydb.Prepare.Put<K, V>(key, value, scala.Option.empty<Deadline>())
    }

    fun <K> remove(key: K): swaydb.Prepare.Remove<K> {
        return swaydb.Prepare.Remove<K>(key, scala.Option.empty<K>(), scala.Option.empty<Deadline>())
    }
}
