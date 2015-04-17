package fourquant.io

import java.io._

import fourquant.utils.IOUtils
import io.scif.config.SCIFIOConfig
import io.scif.config.SCIFIOConfig.ImgMode
import io.scif.img.{ImageRegion, ImgFactoryHeuristic, ImgOpener}
import io.scif.{Metadata, SCIFIO}
import net.imglib2.`type`.numeric.RealType
import net.imglib2.`type`.numeric.real.FloatType
import net.imglib2.`type`.{NativeType, Type}
import net.imglib2.img.array.{ArrayImg, ArrayImgFactory}
import net.imglib2.img.basictypeaccess.array.ArrayDataAccess
import net.imglib2.img.{Img, ImgFactory}
import net.imglib2.meta.Axes
import org.apache.spark.input.PortableDataStream

import scala.reflect.ClassTag

/**
 * Created by mader on 2/27/15.
 */
object ScifioOps extends Serializable {


  /**
   * Functions the Img classes should have that are annoying to write
   * @param img
   * @tparam T the type of the image
   */
  implicit class ImgWithDim[T<: NativeType[T]](img: Img[T]) {
    lazy val sf = new SCIFIO()
    lazy val sfIP = sf.imgUtil().makeSCIFIOImgPlus(img)

    def getDimensions: Array[Long] = {
      val oArray = (0 to img.numDimensions()).map(img.dimension(_)).toArray
      println("\t\tGetting Dimensions:"+oArray.mkString(","))
      oArray
    }

    /**
     * A trimmed version with the trailing dimensions (if it is size 1) removed
     * @note this is needed for the fft tools to work correctly
     * @return the size of the image in each dimension as an array
     */
    def getTrimmedDimensions() = {
      val cDim = getDimensions.reverse
      if(cDim(0)<2) {
        cDim.tail.reverse
      } else {
        cDim.reverse
      }
    }


    /**
     * Extract the primitive array from an image (if there is one)
     * @return an optional array
     */
    def getUnderlyingObj(): Option[Any] = {

      val aImg = sf.imgUtil().getArrayAccess(sfIP)
          aImg.update(null) match {
            case ada: ArrayDataAccess[_] =>
              Some(ada.getCurrentStorageArray)
            case _ =>
              println("The array access is not available for " + aImg)
              None
          }
    }

    /**
     * Statically type the object as well (if you know what it is)
     * @tparam U the type of output eg Array[Float]
     * @return
     */
    def getPrimitiveArray[U](): Option[U] = {
      getUnderlyingObj() match {
        case Some(t: U) => Some(t)
        case Some(a: Any) =>
          println("The underyling object is not of the given type:"+a)
          None
        case None => None
      }
    }
    
  }



  /**
   * The simplist, strongly typed representation of the image
   * @param rawArray the rawArray to store the values in
   * @param dim the dimensions (imglib2 format for the image)
   * @tparam T the type of the image
   */
  case class ArrayWithDim[@specialized(Long, Int, Float, Double) T](
                                                                     rawArray: Array[T],
                                                                     dim: Array[Long]
                                                                     )
  object ArrayWithDim {
    def empty[T](implicit tm: ClassTag[T]) = new ArrayWithDim[T](new Array[T](0),Array(0L))
    def dangerouslyEmpty[T] = new ArrayWithDim[T](null,Array(0L))
  }


  /**
   * A basic class which implements a very basic version of the SparkImage trait
   * @param coreImage the image stored as an 'Either' block
   * @param baseTypeMaker the closure for creating the ImgLib2 type since they are not serializable
   * @param itm the classtag for the T since it arrays need to be generated occasionally
   * @tparam T The type of the primitive used to store data (for serialization)
   * @tparam U The type of the ArrayImg (from ImgLib2)
   */
  class ArraySparkImg[T,U<: NativeType[U]](
      override var coreImage: Either[ArrayWithDim[T],ArrayImg[U,_]])(
    implicit val itm: ClassTag[T], var baseTypeMaker: () => U)
    extends SparkImage[T,U] {
    /**
     * a basic constructor is required for creating these objects directly from an ObjectStream
     * (a dummy empty image which can be populated during the un-serialization)
     * @return a very crippled (NPE's are almost certain) empty image
     */
    @deprecated("Only for un-externalization, otherwise it should be avoided completely","1.0")
    protected[ScifioOps] def this() = this(Left(ArrayWithDim.dangerouslyEmpty[T]))(
      null.asInstanceOf[ClassTag[T]],() => null.asInstanceOf[U])

    override var tm = itm

    /**
     * Apply an operation to the image which may change the content and size, but not type or
     * storage
     * @param op the operation to apply (in img space)
     * @return a new GenericSparkImage ready for further operations
     */
    def applyOp(op: ArrayImg[U,_] => Img[U]) = {
      new ArraySparkImg[T,U](Right(
        op(getImg) match {
          case aImg: ArrayImg[U, _] => aImg
          case _ => throw new IllegalArgumentException("The operation must return the same type" +
            " and img-storage as the original")
        }))
    }

    def changeOp[V, W<: NativeType[W] with RealType[W]](
      op: ArrayImg[U,_] => Img[W])(
          implicit ntm: ClassTag[V], newTypeMaker: () => W) = {
      new ArraySparkImg[V,W](Right(
        op(getImg) match {
          case aImg: ArrayImg[W, _] => aImg
          case _ => throw new IllegalArgumentException("The operation must return the same type" +
            " and img-storage as the original")
        }))(ntm,newTypeMaker)
    }

  }

  /**
   * The basic functionality of a SparkImage which uses the ImgLib2 primitives for storing the
   * images on local machines and sending them around, it then switches to the ArrayWithDim
   * format for saving them to disk.
   * @note T and U must be matched, creating a Double with FloatType will eventually cause hairy
   *       error messages
   * @tparam T The type of the primitive used to store data (for serialization)
   * @tparam U The type of the ArrayImg (from ImgLib2)
   */
  trait SparkImage[T,U <: NativeType[U]]
    extends Externalizable {

    var tm: ClassTag[T]

    var coreImage: Either[ArrayWithDim[T],ArrayImg[U,_]]

    var baseTypeMaker: () => U

    var baseType: U = baseTypeMaker()

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
      cImg.getPrimitiveArray[Array[T]]() match {
            case Some(fArr) =>
              ArrayWithDim(fArr,cImg.getDimensions)
            case None =>
              throw new IllegalArgumentException("Image does not have a primitive array back")
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

    /**
     * custom serialization writes the typeMaker function, the ClassTag and then the ArrayWithDim
     * form of the image to the output
     * @param out the ObjectOutput to write everything to
     */
    @throws[IOException]("if the file doesn't exist")
    override def writeExternal(out: ObjectOutput): Unit = {
      out.writeObject(baseTypeMaker)
      out.writeObject(tm)
      out.writeObject(getArray)
    }

    /**
     * custom serialization for reading in these objects, the order of reading is the typeMaker
     * closure, the ClassTag and then the ArrayWithDim
     * @param in the input stream to read from
     */
    @throws[IOException]("if the file doesn't exist")
    @throws[ClassNotFoundException]("if the class cannot be found")
    override def readExternal(in: ObjectInput): Unit = {
      baseTypeMaker = in.readObject.asInstanceOf[() => U]
      baseType = baseTypeMaker() // reinitialize since it is probably wrong
      tm = in.readObject().asInstanceOf[ClassTag[T]]
      coreImage = Left(in.readObject.asInstanceOf[ArrayWithDim[T]])
    }


  }

  /**
   * Add the ability (hacky) to open images from PortableDataStreams
   * @param imgOp the ImgOpener class to be extended
   */
  implicit class FQImgOpener(imgOp: ImgOpener) {
    import IOUtils._

    def openPDS[T<: RealType[T]](fileName: String, pds: PortableDataStream, af: ImgFactory[T], tp:
    T) = {
      val suffix = fileName.split("[.]").reverse.head
      imgOp.openImgs[T](pds.makeLocal(suffix),af,tp)
    }

    def openPDS(fileName: String, pds: PortableDataStream) = {
      val suffix = fileName.split(".").reverse.head
      imgOp.openImgs(pds.makeLocal(suffix))
    }

    def openPDSRegion[T<: RealType[T] with NativeType[T]](fileName: String,
                                       pds: PortableDataStream,
                                    tp: T, inRegion: ImageRegion) = {
      val suffix = fileName.split("[.]").reverse.head

      val roiConfig = new SCIFIOConfig()

      roiConfig.imgOpenerSetRegion(inRegion)

      //TODO currently this is locked into array image factory
      roiConfig.imgOpenerSetImgFactoryHeuristic(new ImgFactoryHeuristic() {
        override def createFactory[S <: NativeType[S]](metadata: Metadata, imgModes:
        Array[ImgMode], t: S): ImgFactory[S] = new ArrayImgFactory[S]()
      })
      imgOp.openImgs[T](pds.makeLocal(suffix),tp,roiConfig)
    }



    def openPDSRegion2D[T<: RealType[T] with NativeType[T]](fileName: String,
                                       pds: PortableDataStream,
                                       tp: T,
                                       xstart: Int, ystart: Int,
                                          xwidth: Int, ywidth: Int) =
    openPDSRegion(fileName,pds,tp,
      new ImageRegion(Array(Axes.X,Axes.Y),Array(xstart+"-"+(xstart+xwidth-1),
        ystart+"-"+(ystart+ywidth-1)))
    )

    def openRegion[T<: RealType[T] with NativeType[T]](localName: String,
                                                          tp: T, inRegion: ImageRegion) = {
      val roiConfig = new SCIFIOConfig()

      roiConfig.imgOpenerSetRegion(inRegion)

      //TODO currently this is locked into array image factory
      roiConfig.imgOpenerSetImgFactoryHeuristic(new ImgFactoryHeuristic() {
        override def createFactory[S <: NativeType[S]](metadata: Metadata, imgModes:
        Array[ImgMode], t: S): ImgFactory[S] = new ArrayImgFactory[S]()
      })
      imgOp.openImgs[T](localName,tp,roiConfig)
    }

    def openRegion2D[T<: RealType[T] with NativeType[T]](localName: String,
                                                            tp: T,
                                                            xstart: Int, ystart: Int,
                                                            xwidth: Int, ywidth: Int) =
      openRegion(localName,tp,
        new ImageRegion(Array(Axes.X,Axes.Y),Array(xstart+"-"+(xstart+xwidth-1),
          ystart+"-"+(ystart+ywidth-1)))
      )

  }


   private[fourquant] def readPath(path: String) = {
     val sf = new SCIFIO()
    val creader = sf.initializer().initializeReader(path)
    (creader,creader.getMetadata())
  }

  /**
   * Read a region using scifio readers
   * @param path
   * @param pos
   * @param regSize
   * @return
   */
  def readRegion(path: String, pos: Array[Long], regSize: Array[Long]) = {
    val (creader,meta) = readPath(path)
    val sliceMeta = meta.get(0)
    val p = creader.openPlane(0,0, pos,regSize)
    p.getBytes()
  }


  def readRegionAsImg[T<: NativeType[T]](path: String, pos: Array[Long], regSize: Array[Long],
                                         tp: Type[T]) = {
    val sf = new SCIFIO()
    val scnf = new SCIFIOConfig()
    scnf.imgOpenerSetImgFactoryHeuristic(new ImgFactoryHeuristic() {
      override def createFactory[T <: NativeType[T]](metadata: Metadata, imgModes:
      Array[ImgMode], t: T): ImgFactory[T] = new ArrayImgFactory[T]()
    })

    val (creader,meta) = readPath(path)

    val cArrImage = new ArrayImgFactory[FloatType].create(regSize,new FloatType())
    val ip = sf.imgUtil().makeSCIFIOImgPlus(cArrImage)

    val p = creader.openPlane(0,0, pos,regSize)
    sf.planeConverter().getArrayConverter().populatePlane(creader,0,0,p.getBytes(),ip,scnf)

    ip.getImg().asInstanceOf[ArrayImg[T,_]]
  }


}
