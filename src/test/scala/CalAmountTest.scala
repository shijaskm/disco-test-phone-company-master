

class CalAmountTest {
  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("CalAmountTest")
  val testContext = new SparkContext(sparkConf)
  val CalcAmount = new CalcAmount(testContext)

  @Test
  def CalAmount() {
    val testFilePath = "/user/hadoop/calls.txt"

    val df = CalcAmount.CalcAmt(testFilePath)

    assert(df.count() > 0)
    assert(df.agg(sum("AAA")).first.getLong(0) > 100)
    assert(df.agg(sum("BBB")).first.getLong(0) > 300)
  }
}