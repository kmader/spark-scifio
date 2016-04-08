package fourquant.io

import fourquant.ImageSparkInstance
import fourquant.utils.SilenceLogs

/**
 * Created by mader on 4/25/15.
 */
class SingletonTesting extends ImageSparkInstance with SilenceLogs {
  override def useLocal: Boolean = true

  override def bigTests: Boolean = false

  override def useCloud: Boolean = false

  test("Basic Setup") {
    //NOTE this crashes DBC, but works fine on Scala
    import _root_.io.scif.SCIFIO
    import _root_.io.scif.img.ImgOpener

    val t = new ImgOpener()
    val p = new SCIFIO()
    assert(p!=null,"SCIFIO isn't null")
    assert(t!=null,"Image Opener isn't null")
  }
  if (useLocal) {
    test("Proper number of singleton values") {
      val basicList = sc.parallelize(0 to 100,20)
      val allPts = basicList.map{
        iv =>
          SingletonTests.getBirthTime()
      }.collect
      println(allPts.distinct.mkString(", "))
      allPts.distinct.length shouldBe 1 // only one singleton
      val newPts = basicList.map{
          iv =>
            (SingletonTests.getBirthTime(),SingletonTests.getAliveTime)
        }.distinct().collect()
      println(newPts.mkString(", "))
      newPts.length should be > 2
      newPts.length should be < 102
    }


  }

}
