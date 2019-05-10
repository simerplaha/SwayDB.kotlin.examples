/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */
package swaydb.kotlin

import java.io.Serializable
import org.apache.commons.lang3.SerializationUtils
import swaydb.data.slice.Slice
import swaydb.data.slice.`Slice$`

class ApacheSerializer<T : Serializable> : swaydb.serializers.Serializer<T> {
    override fun write(myData: T): Slice<Any> {
        val data = SerializationUtils.serialize(myData)
        return `Slice$`.`MODULE$`.ByteSliceImplicits(`Slice$`.`MODULE$`.create(data.size,
                scala.reflect.`ClassTag$`.`MODULE$`.Any()))
                .addBytes(`Slice$`.`MODULE$`.apply(data, scala.reflect.`ClassTag$`.`MODULE$`.Any()))
    }

    override fun read(data: Slice<Any>?): T {
        val byteSlice = `Slice$`.`MODULE$`.ByteSliceImplicits(data).createReader().readRemaining()
        val result = ByteArray(byteSlice.size())
        var index = 0
        while (index < byteSlice.size()) {
            result[index] = byteSlice.apply(index) as Byte
            index += 1
        }
        return SerializationUtils.deserialize(result)
    }
}
