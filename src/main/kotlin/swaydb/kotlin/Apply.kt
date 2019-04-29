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

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import java.time.Duration
import java.time.LocalDateTime

object Apply {
    fun <V> update(value: V): swaydb.Apply.Map<V> {
        return swaydb.Apply.`Update$`.`MODULE$`.apply(value)
    }

    @Suppress("UNCHECKED_CAST")
    fun <V> expire(expireAt: LocalDateTime): swaydb.Apply.Map<V> {
        val expireAtNano = Duration.between(LocalDateTime.now(), expireAt).nano
        return swaydb.Apply.`Expire$`.`MODULE$`.apply(
                FiniteDuration.create(expireAtNano.toLong(), TimeUnit.NANOSECONDS).fromNow()) as swaydb.Apply.Map<V>
    }

    @Suppress("UNCHECKED_CAST")
    fun <V> remove(): swaydb.Apply.Map<V> {
        return swaydb.Apply.`Remove$`.`MODULE$` as swaydb.Apply.Map<V>
    }

    @Suppress("UNCHECKED_CAST")
    fun <V> nothing(): swaydb.Apply.Map<V> {
        return swaydb.Apply.`Nothing$`.`MODULE$` as swaydb.Apply.Map<V>
    }
}