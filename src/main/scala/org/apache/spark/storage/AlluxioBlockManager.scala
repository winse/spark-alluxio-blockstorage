package org.apache.spark.storage

import java.nio.ByteBuffer

import com.google.common.io.ByteStreams
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.Logging
import org.apache.spark.util.Utils

import scala.util.control.NonFatal

/**
 * @see org.apache.spark.storage.TachyonBlockManager
 */
private[spark] class AlluxioBlockManager extends ExternalBlockManager with Logging {

  private var chroot: Path = _
  private var subDirs: Array[Path] = _

  private var fs: FileSystem = _

  override def toString = "ExternalBlockStore-Alluxio"

  override def init(blockManager: BlockManager, executorId: String): Unit = {
    super.init(blockManager, executorId)

    val conf = blockManager.conf

    val masterUrl = conf.get(ExternalBlockStore.MASTER_URL, "alluxio://localhost:19998")
    val storeDir = conf.get(ExternalBlockStore.BASE_DIR, "/tmp_spark_alluxio")
    val folderName = conf.get(ExternalBlockStore.FOLD_NAME)
    val subDirsPerAlluxio = conf.getInt(
      "spark.externalBlockStore.subDirectories",
      ExternalBlockStore.SUB_DIRS_PER_DIR.toInt)

    val master = new Path(masterUrl)
    chroot = new Path(master, s"$storeDir/$folderName/$executorId")
    fs = master.getFileSystem(new Configuration)
    fs.mkdirs(chroot)
    fs.deleteOnExit(chroot.getParent)

    subDirs = new Array[Path](subDirsPerAlluxio)
  }

  override def putBytes(blockId: BlockId, bytes: ByteBuffer) = {
    val output = fs.create(getFile(blockId), true)
    try {
      output.write(bytes.array())
    } catch {
      case NonFatal(e) =>
        logWarning(s"Failed to put values of block $blockId into Alluxio", e)
    } finally {
      output.close()
    }
  }

  /**
   * this method must be override, alluxio not support write append.
   */
  override def putValues(blockId: BlockId, values: Iterator[_]): Unit = {
    val output = fs.create(getFile(blockId), true)
    try {
      blockManager.dataSerializeStream(blockId, output, values)
    } catch {
      case NonFatal(e) =>
        logWarning(s"Failed to put values of block $blockId into Alluxio", e)
    } finally {
      output.close()
    }
  }

  override def getBytes(blockId: BlockId): Option[ByteBuffer] = {
    val path = getFile(blockId)
    if (!fs.exists(path)) {
      None
    } else {
      val size = fs.getFileStatus(path).getLen
      if (size == 0) {
        None
      } else {
        val input = fs.open(path)
        try {
          val buffer = new Array[Byte](size.toInt)
          ByteStreams.readFully(input, buffer)
          Some(ByteBuffer.wrap(buffer))
        } catch {
          case NonFatal(e) =>
            logWarning(s"Failed to get bytes of block $blockId from Alluxio", e)
            None
        } finally {
          input.close()
        }
      }
    }
  }

  override def getValues(blockId: BlockId): Option[Iterator[_]] = {
    val bytes: Option[ByteBuffer] = getBytes(blockId)
    bytes.map(bs =>
      // alluxio.hadoop.HdfsFileInputStream#available unsupport!
      // blockManager.dataDeserialize(blockId, input)

      blockManager.dataDeserialize(blockId, bs)
    )
  }

  override def getSize(blockId: BlockId): Long =
    fs.getFileStatus(getFile(blockId)).getLen

  override def blockExists(blockId: BlockId): Boolean =
    fs.exists(getFile(blockId))

  override def removeBlock(blockId: BlockId): Boolean =
    fs.delete(getFile(blockId), false)

  override def shutdown(): Unit = {
    fs.close();
  }

  private def getFile(blockId: BlockId): Path = getFile(blockId.name)

  private def getFile(filename: String): Path = {
    val hash = Utils.nonNegativeHash(filename)
    val subDirId = hash % subDirs.length

    var subDir = subDirs(subDirId)
    if (subDir == null) {
      val path = new Path(chroot, subDirId.toString)
      fs.mkdirs(path)

      subDirs(subDirId) = path
      subDir = path
    }

    new Path(subDir, filename)
  }

}
