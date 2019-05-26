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
package swaydb.base

import java.io.IOException
import java.nio.file.FileVisitResult
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.SimpleFileVisitor
import java.nio.file.attribute.BasicFileAttributes

open class TestBase {

    fun addTarget(path: Path): Path {
        return Paths.get("target", path.toFile().path)
    }

    fun deleteDirectoryWalkTree(path: Path) {
        if (!path.toFile().exists()) {
            return
        }
        val visitor = object : SimpleFileVisitor<Path>() {

            @Throws(IOException::class)
            override fun visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult {
                Files.delete(file)
                return FileVisitResult.CONTINUE
            }

            @Throws(IOException::class)
            override fun visitFileFailed(file: Path, exc: IOException?): FileVisitResult {
                Files.delete(file)
                return FileVisitResult.CONTINUE
            }

            @Throws(IOException::class)
            override fun postVisitDirectory(dir: Path, exc: IOException?): FileVisitResult {
                if (exc != null) {
                    return FileVisitResult.TERMINATE
                }
                Files.delete(dir)
                return FileVisitResult.CONTINUE
            }
        }
        try {
            Files.walkFileTree(path, visitor)
        } catch (ignored: IOException) {
            // ignored
        }

    }

    @Throws(IOException::class)
    fun deleteDirectoryWalkTreeStartsWith(startName: String) {
        Files.walk(Paths.get("."))
                .filter { file -> file.toFile().isDirectory }
                .filter { s -> s.nameCount == 2 }
                .filter { s -> s.getName(1).toString().startsWith(startName) }
                .map<Path>({ it.getFileName() })
                .sorted()
                .forEach{ deleteDirectoryWalkTree(it) }
    }

}
