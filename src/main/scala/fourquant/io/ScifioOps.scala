package fourquant.io

import java.io._

import io.scif.img.ImgOpener
import net.imglib2.`type`.NativeType
import net.imglib2.`type`.numeric.RealType
import net.imglib2.`type`.numeric.real.FloatType
import net.imglib2.img.array.{ArrayImg, ArrayImgFactory}
import net.imglib2.img.basictypeaccess.array.ArrayDataAccess
import net.imglib2.img.{Img, ImgFactory}
import org.apache.spark.input.PortableDataStream

import scala.reflect.ClassTag

/**
 * Created by mader on 2/27/15.
 */
object ScifioOps extends Serializable {

  /**
   * Functions the Img classes should have that are annoying to write
   * @param img
   * @tparam U
   */
  implicit class ImgWithDim[U <: Img[_]](img: U) {

    def getDimensions: Array[Long] =
      (0 to img.numDimensions()).map(img.dimension(_)).toArray
  }

  case class ArrayWithDim[@specialized(Long, Int, Float, Double) T](
                                                                     rawArray: Array[T],
                                                                     dim: Array[Long]
                                                                     )
  object ArrayWithDim {
    def empty[T](implicit tm: ClassTag[T]) = new ArrayWithDim[T](new Array[T](0),Array(0L))
  }

  /**
   *
   * @tparam T The type of the primitive used to store data (for serialization)
   * @tparam U The type of the ArrayImg (from ImgLib2)
   */
  abstract class SparkImage[T,U <: NativeType[U] with RealType[U]](
              var coreImage: Either[ArrayWithDim[T],ArrayImg[U,_]])
                                                                  (implicit tm: ClassTag[T])
    extends Externalizable {

    def this()(implicit tm: ClassTag[T]) = this(Left(ArrayWithDim.empty[T]))

    val baseType: U

    /**
     * These methods are pretty basic and just support primitives and their corresponding ImgLib2
     * types
     * @param dimArray
     * @return
     */
    protected def arrayToImg(dimArray: ArrayWithDim[T]): ArrayImg[U,_] = {
      val cImg = new ArrayImgFactory[U]().create(dimArray.dim, baseType)
      cImg.update(null) match {
        case ada: ArrayDataAccess[_] =>
          java.lang.System.arraycopy(dimArray.rawArray, 0,
            ada.getCurrentStorageArray, 0, dimArray.rawArray.length)
        case _ => throw new IllegalAccessException("The array access is not available for " + cImg)
      }
      cImg
    }

    protected def imgToArray(cImg: ArrayImg[U,_]): ArrayWithDim[T] = {
      cImg.update(null) match {
        case ada: ArrayDataAccess[_] =>
          ada.getCurrentStorageArray match {
            case fArr: Array[T] =>
              ArrayWithDim(fArr,cImg.getDimensions)
            case junk: AnyRef =>
              throw new IllegalAccessException(cImg+" has an unexpected type backing "+junk)
          }
        case _ =>
          throw new IllegalAccessException("The array access is not available for " + cImg)
      }
    }

    private def calcImg = coreImage match {
      case Left(dimArray) => arrayToImg(dimArray)
      case Right(gImg) => gImg
    }

    private def calcArr = coreImage match {
      case Left(dimArray) => dimArray
      case Right(cImg) => imgToArray(cImg)
    }

    def numDimensions = coreImage match {
      case Left(dArray) => dArray.dim.length
      case Right(cImg) => cImg.numDimensions
    }

    def dimension(i: Int): Long =  coreImage match {
      case Left(dArray) => dArray.dim(i)
      case Right(cImg) => cImg.dimension(i)
    }

    lazy val getImg = calcImg
    lazy val getArray = calcArr

    // custom serialization
    @throws[IOException]("if the file doesn't exist")
    override def writeExternal(out: ObjectOutput): Unit = out.writeObject(getArray)

    @throws[IOException]("if the file doesn't exist")
    @throws[ClassNotFoundException]("if the class cannot be found")
    override def readExternal(in: ObjectInput): Unit = {
      coreImage = Left(in.readObject.asInstanceOf[ArrayWithDim[T]])
    }


  }


  class SparkFloatImg(coreImage: Either[ArrayWithDim[Float],ArrayImg[FloatType,_]])
    extends SparkImage[Float,FloatType](coreImage) {

    override val baseType = new FloatType

    def this(ifimg: ArrayImg[FloatType,_]) = this(Right(ifimg))

    def this(rawArray: Array[Float], dim: Array[Long]) = this(Left(ArrayWithDim(rawArray, dim)))

    def this() = this(new Array[Float](0),Array(0L))
  }

  implicit class FQImgOpener(imgOp: ImgOpener) {

    private def flattenPDS(pds: PortableDataStream, suffix: String) = {
      val bais = new ByteArrayInputStream(pds.toArray())
      val tempFile = File.createTempFile("SparkScifioOps","."+suffix)
      org.apache.commons.io.IOUtils.copy(bais,new FileOutputStream(tempFile))
      tempFile.getAbsolutePath
    }

    def openPDS[T<: RealType[T]](fileName: String, pds: PortableDataStream, af: ImgFactory[T], tp:
    T) = {
      val suffix = fileName.split("[.]").reverse.head
      imgOp.openImgs[T](flattenPDS(pds,suffix),af,tp)
    }

    def openPDS(fileName: String, pds: PortableDataStream) = {
      val suffix = fileName.split(".").reverse.head
      imgOp.openImgs(flattenPDS(pds,suffix))
    }

  }

}
