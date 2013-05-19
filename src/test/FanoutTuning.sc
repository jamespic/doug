import uk.me.jamespic.dougng.util._
object FanoutTuning {
  val values = (1 to 1000000) map (i => (i, i.toDouble))
                                                  //> values  : scala.collection.immutable.IndexedSeq[(Int, Double)] = Vector((1,1
                                                  //| .0), (2,2.0), (3,3.0), (4,4.0), (5,5.0), (6,6.0), (7,7.0), (8,8.0), (9,9.0),
                                                  //|  (10,10.0), (11,11.0), (12,12.0), (13,13.0), (14,14.0), (15,15.0), (16,16.0)
                                                  //| , (17,17.0), (18,18.0), (19,19.0), (20,20.0), (21,21.0), (22,22.0), (23,23.0
                                                  //| ), (24,24.0), (25,25.0), (26,26.0), (27,27.0), (28,28.0), (29,29.0), (30,30.
                                                  //| 0), (31,31.0), (32,32.0), (33,33.0), (34,34.0), (35,35.0), (36,36.0), (37,37
                                                  //| .0), (38,38.0), (39,39.0), (40,40.0), (41,41.0), (42,42.0), (43,43.0), (44,4
                                                  //| 4.0), (45,45.0), (46,46.0), (47,47.0), (48,48.0), (49,49.0), (50,50.0), (51,
                                                  //| 51.0), (52,52.0), (53,53.0), (54,54.0), (55,55.0), (56,56.0), (57,57.0), (58
                                                  //| ,58.0), (59,59.0), (60,60.0), (61,61.0), (62,62.0), (63,63.0), (64,64.0), (6
                                                  //| 5,65.0), (66,66.0), (67,67.0), (68,68.0), (69,69.0), (70,70.0), (71,71.0), (
                                                  //| 72,72.0), (73,73.0), (74,74.0), (75,75.0), (76,76.0), (77,77.0), (78,78.0), 
                                                  //| (79,79.0), (80,80.0), (8
                                                  //| Output exceeds cutoff limit.
  for (i <- 3 to 6) {
    System.gc()
    System.gc()
    val fanout = 1 << i
    val startTime = System.currentTimeMillis
    val mrqs = new MapReduceQuickSort[Int, Double, Double](values, identity, _ + _, fanout)
    val buildTime = System.currentTimeMillis
    for (i <- 0 to 99) {
      mrqs.summaryBetween(10000 * i + 1, 10000 * i + 10000)
    }
    val endTime = System.currentTimeMillis
    println(s"With fanout $fanout, built in ${buildTime - startTime}ms, queried in ${endTime - buildTime}ms")
  }                                               //> With fanout 8, built in 9822ms, queried in 1760ms
                                                  //| With fanout 16, built in 5645ms, queried in 187ms
                                                  //| With fanout 32, built in 4947ms, queried in 67ms
                                                  //| With fanout 64, built in 5218ms, queried in 246ms
}