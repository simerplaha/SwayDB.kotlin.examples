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
import swaydb.data.slice.Slice
import swaydb.data.slice.`Slice$`

/**
 * The BytesReader wrapper.
 */
class BytesReader private constructor(slice: Slice<Any>) {

    private val reader: swaydb.data.slice.BytesReader

    init {
        reader = `Slice$`.`MODULE$`.ByteSliceImplicits(slice).createReader()
    }

    /**
     * Reads the string data.
     * @param size the size
     *
     * @return the string data
     */
    fun readString(size: Int): String {
        return reader.readString(size, StandardCharsets.UTF_8)
    }

    /**
     * Reads the int data.
     *
     * @return the int data
     */
    fun readInt(): Int {
        return reader.readInt()
    }

    /**
     * Reads the long data.
     *
     * @return the long data
     */
    fun readLong(): Long {
        return reader.readLong()
    }

    /**
     * Reads the byte data.
     *
     * @return the byte data
     */
    fun readByte(): Byte {
        return reader.read(1).apply(0) as Byte
    }

    /**
     * Reads the boolean data.
     *
     * @return the boolean data
     */
    fun readBoolean(): Boolean {
        return (reader.read(1).apply(0) as Byte).toInt() == 1
    }

    companion object {

        /**
         * Creates the BytesReader object.
         * @param slice the slice
         *
         * @return the BytesReader object
         */
        fun create(slice: Slice<Any>): BytesReader {
            return BytesReader(slice)
        }
    }
}
