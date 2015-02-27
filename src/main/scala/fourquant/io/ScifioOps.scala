package fourquant.io

import java.io._

import io.scif.img.ImgOpener
import net.imglib2.`type`.numeric.RealType
import net.imglib2.`type`.numeric.real.FloatType
import net.imglib2.img.array.{ArrayImgFactory, ArrayImg}
import net.imglib2.img.basictypeaccess.array.ArrayDataAccess
import net.imglib2.img.{Img, ImgFactory}
import org.apache.spark.input.PortableDataStream

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

  trait SparkImage[T,U] extends Serializable {
    var coreImage: Either[ArrayWithDim[T],ArrayImg[U,_]]
    def arrayToImg(inArr: ArrayWithDim[T]): ArrayImg[U,_]
    def imgToArray(inImg: ArrayImg[U,_]): ArrayWithDim[T]

    private def calcImg = coreImage match {
      case Left(dimArray) => arrayToImg(dimArray)
      case Right(gImg) => gImg
    }

    private def calcArr = coreImage match {
      case Left(dimArray) => dimArray
      case Right(cImg) => imgToArray(cImg)
    }


    lazy val RawImg = calcImg
    lazy val RawArray = calcArr
  }

  class SparkFloatImg(var coreImage: Either[ArrayWithDim[Float],ArrayImg[FloatType,_]]) extends
    SparkImage[Float,FloatType] {
    def this(ifimg: ArrayImg[FloatType,_]) = this(Right(ifimg))

    def this(rawArray: Array[Float], dim: Array[Long]) = this(Left(ArrayWithDim(rawArray, dim)))

    override def arrayToImg(dimArray: ArrayWithDim[Float]): ArrayImg[FloatType, _] = {
      val cImg = new ArrayImgFactory[FloatType]().create(dimArray.dim, new FloatType)
      cImg.update(null) match {
        case ada: ArrayDataAccess[_] =>
          java.lang.System.arraycopy(dimArray.rawArray, 0,
            ada.getCurrentStorageArray, 0, dimArray.rawArray.length)
        case _ => throw new IllegalAccessException("The array access is not available for " + cImg)
      }
      cImg
    }

    override def imgToArray(cImg: ArrayImg[FloatType, _]): ArrayWithDim[Float] = {
      cImg.update(null) match {
        case ada: ArrayDataAccess[_] =>
          ada.getCurrentStorageArray match {
            case fArr: Array[Float] =>
              ArrayWithDim(fArr,cImg.getDimensions)
            case junk: AnyRef =>
              throw new IllegalAccessException(cImg+" has an unexpected type backing "+junk)
          }
        case _ =>
          throw new IllegalAccessException("The array access is not available for " + cImg)
      }
    }

    // custom serialization
    @throws[IOException]("if the file doesn't exist")
    private def writeObject(oos: ObjectOutputStream): Unit = {
      oos.writeObject(RawArray)
    }
    @throws[IOException]("if the file doesn't exist")
    @throws[ClassNotFoundException]("if the class cannot be found")
    private def readObject(in: ObjectInputStream): Unit =  {
      coreImage = Left(in.readObject.asInstanceOf[ArrayWithDim[Float]])
    }
    @throws(classOf[ObjectStreamException])
    private def readObjectNoData: Unit = {
      throw new IllegalArgumentException("Cannot have a dataless SparkFloatImg");
    }

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
