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

import java.nio.charset.StandardCharsets
import swaydb.data.slice.`Slice$`

/**
 * The Slice wrapper.
 */
class Slice private constructor(size: Int) {

    private var sliceObject: swaydb.data.slice.Slice<Any>? = null

    init {
        sliceObject = `Slice$`.`MODULE$`.create(size, scala.reflect.`ClassTag$`.`MODULE$`.Any())
    }

    /**
     * Adds string to the Slice object.
     * @param string the string
     *
     * @return the Slice object
     */
    fun addString(string: String): Slice {
        sliceObject = `Slice$`.`MODULE$`.ByteSliceImplicits(sliceObject)
                .addString(string, StandardCharsets.UTF_8)
        return this
    }

    /**
     * Adds int to the Slice object.
     * @param value the value
     *
     * @return the Slice object
     */
    fun addInt(value: Int): Slice {
        sliceObject = `Slice$`.`MODULE$`.ByteSliceImplicits(sliceObject).addInt(value)
        return this
    }

    /**
     * Adds long to the Slice object.
     * @param value the value
     *
     * @return the Slice object
     */
    fun addLong(value: Long): Slice {
        sliceObject = `Slice$`.`MODULE$`.ByteSliceImplicits(sliceObject).addLong(value)
        return this
    }

    /**
     * Adds byte to the Slice object.
     * @param value the value
     *
     * @return the Slice object
     */
    fun addByte(value: Byte): Slice {
        sliceObject = `Slice$`.`MODULE$`.ByteSliceImplicits(sliceObject).addByte(value)
        return this
    }

    /**
     * Adds boolean to the Slice object.
     * @param value the value
     *
     * @return the Slice object
     */
    fun addBoolean(value: Boolean): Slice {
        sliceObject = `Slice$`.`MODULE$`.ByteSliceImplicits(sliceObject).addBoolean(value)
        return this
    }

    /**
     * Closes the Slice object.
     *
     * @return the Slice object
     */
    fun close(): swaydb.data.slice.Slice<Any> {
        return sliceObject!!.close()
    }

    companion object {

        /**
         * Creates the Slice object.
         * @param size the size
         *
         * @return the Slice object
         */
        fun create(size: Int): Slice {
            return Slice(size)
        }
    }
}
