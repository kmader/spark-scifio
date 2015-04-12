package fourquant.utils

import java.io._

import org.apache.spark.input.PortableDataStream

/**
 * Created by mader on 4/12/15.
 */
object IOUtils {
  /**
   *
   * @param filepath the given file path
   * @return if it is a local file
   */
   def isPathLocal(filepath: String): Boolean = {
    try {
      new File(filepath).exists()
    } catch {
      case _ => false
    }
  }


  /**
   * Adds some functionality to portable data stream useful for integrating with SCIFIO and other
   * tools
   * @param pds
   */
  implicit class LocalPortableDataStream(pds: PortableDataStream) extends Serializable {
    def getTrimmedPath() =
      pds.getPath().replaceAll("file:","")

    def cache(): InputStream =
      new ByteArrayInputStream(pds.toArray())

    def isLocal() = isPathLocal(pds.getTrimmedPath())

    /**
     * Provides a local path for opening a PortableDataStream
     * @param suffix for the file since some readers still check this
     * @param basePath the location to save the file
     * @return the path to the local file to read
     */
    def makeLocal(suffix: String, basePath: Option[String] = None) = {
      if (pds.isLocal()) {
        pds.getTrimmedPath()
      } else {
        println("Copying PDS Resource....")
        val bais = new ByteArrayInputStream(pds.toArray())
        val tempFile = basePath match {
          case Some(folderPath) =>
            File.createTempFile("SparkScifioOps","."+suffix,new File(folderPath))
          case None =>
            File.createTempFile("SparkScifioOps","."+suffix)
        }
        org.apache.commons.io.IOUtils.copy(bais,new FileOutputStream(tempFile))
        tempFile.getAbsolutePath
      }
    }
  }

}
