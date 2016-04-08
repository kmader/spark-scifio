package fourquant.io

import _root_.io.scif.config.SCIFIOConfig
import _root_.io.scif.config.SCIFIOConfig.ImgMode
import _root_.io.scif.img.{ImageRegion, ImgFactoryHeuristic, ImgOpener}
import _root_.io.scif.{Metadata, SCIFIO}
import fourquant.io.IOOps._
import fourquant.io.ScifioOps.ArraySparkImg
import fourquant.utils.SilenceLogs
import net.imagej.axis.Axes
import net.imglib2.`type`.NativeType
import net.imglib2.`type`.numeric.RealType
import net.imglib2.`type`.numeric.real.{DoubleType, FloatType}
import net.imglib2.img.ImgFactory
import net.imglib2.img.array.{ArrayImg, ArrayImgFactory}
import org.apache.spark.SparkContext
import org.scalatest.{FunSuite, Matchers}

import scala.collection.JavaConversions._

class LargeImageIOTests extends FunSuite with Matchers with SilenceLogs {
  lazy val sc = new SparkContext("local[4]","Test")
  val testDataDir = "/Users/mader/Dropbox/Informatics/spark-imageio/test-data/"
  val bigImage = testDataDir + "Hansen_GFC2014_lossyear_00N_000E.tif"


  val io = new ImgOpener()


  def checkTestImage[U <: NativeType[U] with RealType[U] ](firstImage: ArrayImg[U,_],
                                                            xdim: Int = 100, ydim: Int = 100,
                                                            checkValues: Boolean = true):
  Unit = {
    assert(firstImage.dimension(0)==xdim,"has the right width")
    assert(firstImage.dimension(1)==ydim,"has the right height")
    firstImage.numDimensions() match {
      case 2 => "Alright"
      case 3 => assert(firstImage.dimension(2)==1,"has only one slice")
      case 4 =>
        assert(firstImage.dimension(2)==1,"has only one slice")
        assert(firstImage.dimension(3)==1,"has only one slice")
      case _ =>
        assert(firstImage.numDimensions()>3,"Number of dimensions is too high")
    }
    if (checkValues) {
      val imgIt =  firstImage.iterator()
      assert(imgIt.next().getRealDouble == 65535.0, "The first value")
      imgIt.next()
      assert(imgIt.next().getRealDouble == 0.0, "The third value")
    }
  }

  test("Use the Reader class") {
    val sf = new SCIFIO()
    val scnf = new SCIFIOConfig()

    val creader = sf.initializer().initializeReader(bigImage)

    val meta = creader.getMetadata()
    println(creader+", "+meta)
    creader.getImageCount shouldBe 1
    val imet = meta.get(0)
    creader.getPlaneCount(0) shouldBe 1
    imet.getAxesLengths()(0) shouldBe 40000
    imet.getAxesLengths()(1) shouldBe 40000
    val planeSize = Array(2000L,2000L)
    val p = creader.openPlane(0,0, Array(36000L,8000L),planeSize)

    p.getLengths()(0) shouldBe planeSize(0)
    p.getLengths()(1) shouldBe planeSize(1)

    p.getBytes().length shouldBe planeSize(0)*planeSize(1)
    p.getBytes().map(_.toDouble).sum shouldBe 1785

    val cArrImage = new ArrayImgFactory[FloatType].create(planeSize,new FloatType())

    val ip = sf.imgUtil().makeSCIFIOImgPlus(cArrImage)
    sf.planeConverter().getArrayConverter().populatePlane(creader,0,0,p.getBytes(),ip,scnf)

    import ScifioOps._
    ip.getImg().getPrimitiveArray[Array[Float]]() match {
      case Some(arr) =>
        arr.length shouldBe planeSize(0)*planeSize(1)
        arr.sum shouldBe 1785
      case None => throw new RuntimeException("Image could not be read")
    }
  }

  test("Read in a small piece of the whole image") {
    val roiConfig = new SCIFIOConfig()
    val curRegion = new ImageRegion(Array(Axes.X,Axes.Y),Array("0-100","0-100"))
    roiConfig.imgOpenerSetRegion(curRegion)
    roiConfig.imgOpenerSetImgFactoryHeuristic(new ImgFactoryHeuristic() {
      override def createFactory[T <: NativeType[T]](metadata: Metadata, imgModes:
      Array[ImgMode], t: T): ImgFactory[T] = new ArrayImgFactory[T]()
    })

    val inImage = io.openImgs(bigImage,new FloatType,roiConfig)
    assert(inImage.size()==1,"There is only one image in the file")
    val firstImage = inImage.head
    checkTestImage(firstImage.getImg().asInstanceOf[ArrayImg[FloatType,_]],101,101,false)

    import ScifioOps._
    firstImage.getImg().getPrimitiveArray[Array[Float]]() match {
      case Some(arr) =>
        println("Array Loaded:"+arr.sum+" from:"+arr.min+" to "+arr.max)
      case None =>
        throw new RuntimeException("Array cannot be loaded!")
    }

  }

  test("Read in a small part in spark") {
    val roiImages =
      sc.genericArrayImagesRegion2D[Array[Float],FloatType](bigImage,3,
        Array((0,0,100,100),(100,100,100,100)))
    roiImages.count() shouldBe 2

    val firstImage = roiImages.first._2.getImg

    checkTestImage(firstImage.asInstanceOf[ArrayImg[FloatType, _]],100,100,false)
  }


val newish = false
  val testImgPath = ""
  if (newish) {
    test("Read a fake image generically spark") {
      val dtg = () => new DoubleType
      val pImgData = sc.genericArrayImages[Double, DoubleType](testImgPath).cache

      assert(pImgData.count == 1, "only one image")

      val firstImage = pImgData.first._2.getImg

      checkTestImage(firstImage.asInstanceOf[ArrayImg[DoubleType, _]])

    }

    test("Read and play with a generic image") {
      val dtg = () => new DoubleType
      val pImgData = sc.genericArrayImages[Double, DoubleType](testImgPath).cache
      val indexData = pImgData.map(_._2).flatMap {
        inKV => for (i <- 0 to 5) yield (i, inKV)
      }

      val mangledData = indexData.cartesian(indexData).filter(a => a._1._1 == (a._2._1 + 1))

      assert(mangledData.count == 5, "5 images can be mapped from n->n+1")

      val firstImage = mangledData.first._1._2.getImg

      checkTestImage(firstImage.asInstanceOf[ArrayImg[DoubleType, _]])
    }

    test("Read a float image in spark") {
      val pImgData = sc.floatImages(testImgPath).cache

      assert(pImgData.count == 1, "only one image")

      val firstImage = pImgData.first._2.getImg
      checkTestImage(firstImage.asInstanceOf[ArrayImg[FloatType, _]])

    }

    test("Read a int image in spark") {
      val pImgData = sc.intImages(testImgPath).cache

      assert(pImgData.count == 1, "only one image")

      val firstImage = pImgData.first._2.getImg
      checkTestImage(firstImage.asInstanceOf[ArrayImg[FloatType, _]])

    }

    test("Read a double image in spark") {
      val pImgData = sc.doubleImages(testImgPath).cache

      assert(pImgData.count == 1, "only one image")

      val firstImage = pImgData.first._2.getImg
      checkTestImage(firstImage.asInstanceOf[ArrayImg[FloatType, _]])

    }

    test("Gaussian filter a float image") {
      val pImgData = sc.floatImages(testImgPath).
        mapValues { iImg =>
        new ArraySparkImg(
          Right(
            net.imglib2.algorithm.gauss.Gauss.toFloat(Array(3.0, 3.0), iImg.getImg).
              asInstanceOf[ArrayImg[FloatType, _]]
          )
        )
      }

      pImgData.count should equal(1)

      val firstImage = pImgData.first._2.getImg

      firstImage.firstElement().getRealDouble should equal(9720.0 +- 100)

    }

    test("Gaussian apply op") {
      val gaussianOp =
        (x: ArrayImg[FloatType, _]) => net.imglib2.algorithm.gauss.Gauss.toFloat(Array(3.0, 3.0), x)
      val pImgData = sc.floatImages(testImgPath).
        mapValues(iImg => iImg.applyOp(gaussianOp))

      assert(pImgData.count == 1, "only one image")

      val firstImage = pImgData.first._2.getImg

      firstImage.firstElement().getRealDouble should equal(9720.0 +- 100)

    }

    test("Read a big image in spark") {

      val pImgData = sc.doubleImages("/Users/mader/Dropbox/4Quant/Volume_Viewer_2.tif").cache

      pImgData.count should equal(1)

      val firstImage = pImgData.first._2.getImg.asInstanceOf[ArrayImg[FloatType, _]]
      firstImage.dimension(0) should equal(684)

      firstImage.dimension(1) should equal(800)

    }
  }

}


