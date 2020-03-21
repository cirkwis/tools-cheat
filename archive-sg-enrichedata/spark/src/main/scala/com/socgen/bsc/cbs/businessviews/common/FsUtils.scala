package com.socgen.bsc.cbs.businessviews.common

import java.io.{BufferedWriter, OutputStreamWriter, _}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, _}

/**
  * Created by X153279 on 28/10/2016.
  */
object FsUtils {

  /* org.apache.hadoop.fs.RemoteIterator is not an iterator, so here is an adapter for convenience */
  implicit class RemoteIteratorToIterator[T](it: RemoteIterator[T]) extends Iterator[T] {
    def next() = it.next()
    def hasNext = it.hasNext
  }

  private val hdfs: FileSystem = FileSystem.get(new Configuration)

  def exists(path: Path): Boolean = {
    hdfs.exists(path)
  }

  def listDirs(path: Path): Iterable[Path] = {
    hdfs.listFiles(path, true)
      .map(_.getPath.getParent)
      .filter(!_.getName.startsWith("_"))
      .toSet
  }

  @throws(classOf[IOException])
  def writeAllLines(lines: Iterable[String], path: Path) {
    val br = new BufferedWriter(new OutputStreamWriter(hdfs.create(path, true)))
    for (line <- lines) {
      if (line != null) {
        br.write(line)
        br.write("\n")
        br.flush()
      }
    }
  }

  @throws(classOf[IOException])
  def write(text: String, path: Path): Unit = {
    val br = new BufferedWriter(new OutputStreamWriter(hdfs.create(path, true)))
    br.write(text)
    br.flush()
  }

}
