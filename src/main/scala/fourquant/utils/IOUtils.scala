package fourquant.utils

import java.io._

import org.apache.spark.SparkConf
import org.apache.spark.input.PortableDataStream
import org.apache.commons.io.IOUtils.toByteArray

import scala.reflect.ClassTag

/**
 * Created by mader on 4/12/15.
 */
object IOUtils {

  var localTmpPath = new SparkConf().getOption("spark.local.dir")

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
   * Finally converting the portableobject architecture into a more usuable design pattern. It
   * holds
   * @param coreData the underlying datastructure holding either the serialized object or the
   *                 useful version, or null
   * @tparam A the useful type which is not serializable
   * @tparam B the serializable type which is generally not useful
   */
  abstract class PortableObject[A,B <: Serializable](var coreData: Either[A,B])(
    implicit cta: ClassTag[A], ctb: ClassTag[B])
    extends Externalizable {

    private[IOUtils] def this()(implicit cta: ClassTag[A], ctb: ClassTag[B]) = this(null)

    lazy val lazyA = coreData match {
      case Left(a) => a
      case Right(b) => fromBtoA(b)
    }
    lazy val lazyB = coreData match {
      case Left(a) => fromAtoB(a)
      case Right(b) => b
    }

    private[IOUtils] def fromAtoB(a: A): B
    private[IOUtils] def fromBtoA(b: B): A

    def getUseful(): A = lazyA
    def getSerializable(): B = lazyB

    override def readExternal(in: ObjectInput): Unit = {
      coreData = in.readObject() match {
        case bser: B =>  Right(bser)
        case _ => throw new RuntimeException("Object "+this.getClass.getCanonicalName+" cannot be" +
          " deserialized")
      }
    }

    override def writeExternal(out: ObjectOutput): Unit = {
      out.writeObject(getSerializable())
    }
  }

  class PortableByteStream(var inValue: Either[ByteArrayInputStream,Array[Byte]])
    extends PortableObject[ByteArrayInputStream,Array[Byte]](inValue) {

    private[IOUtils] def this() = this(null)

    override private[IOUtils] def fromAtoB(a: ByteArrayInputStream): Array[Byte] =
      toByteArray(a)

    override private[IOUtils] def fromBtoA(b: Array[Byte]): ByteArrayInputStream =
      new ByteArrayInputStream(b)
  }


  /**
   * Adds some functionality to portable data stream useful for integrating with SCIFIO and other
   * tools
   * @param pds
   */
  implicit class LocalPortableDataStream(pds: PortableDataStream) extends Serializable {
    def getTrimmedPath() =
      pds.getPath().replaceAll("file:","")

    def getSuffix() = pds.getPath().split("[.]").reverse.headOption

    def cache() = new PortableByteStream(Right(pds.toArray()))
      //new ByteArrayInputStream(pds.toArray())

    def isLocal() = isPathLocal(pds.getTrimmedPath())

    /**
     * Provides a local path for opening a PortableDataStream
     * @param suffix for the file since some readers still check this
     * @param basePath the location to save the file
     * @return the path to the local file to read
     */
    def makeLocal(suffix: String, basePath: Option[String] = localTmpPath) = {
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
