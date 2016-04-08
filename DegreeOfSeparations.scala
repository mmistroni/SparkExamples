import org.apache.spark.SparkContext
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._
import scala.collection.mutable.MutableList
import scala.util.control.Breaks._
        

object DegreeOfSeparations {
  
  
  
  
  
  def calculateDegreeOfSeparations(sc:SparkContext):Unit = {
      val startCharacterID = 5306 //SpiderMan
      val targetCharacterID = 14  //ADAM 3,031 (who?)
      val hitCounter = sc.accumulator(0)

 
      def convertToBFS(line:String):(Int, (List[String], Int, String)) = {
        val fields = line.split(" ")
        val heroID = fields(0).toInt
        val connections = fields.slice(1, fields.size).toList
        
        val (color, distance) = heroID match {
          case `startCharacterID` => ("GRAY", 0)
          case _ => ("WHITE", 9999)
        }
        return (heroID, (connections, distance, color))
      }

      def bfsReduce(data1:(List[String], Int, String), 
                    data2:(List[String], Int, String)) = {
          val (edges1, distance1, color1) = data1
          val (edges2, distance2, color2)= data2
          
          var distance = 9999
          var color = "WHITE"
          var edges = List[String]()
      
          //# See if one is the original node with its connections.
          //# If so preserve them.
          if (edges1.size > 0) {
              edges = edges1
          } 
          if (edges2.size > 0){
              val seq = for (connection <- edges2) yield {connection}
              edges = seq.toList
          }
          //# Preserve minimum distance
          if (distance1 < distance) {
              distance = distance1
          }
      
          if (distance2 < distance) {
              distance = distance2
          }
          //# Preserve darkest color
          if (color1.equals("WHITE") &&  (color2.equals("GRAY") || color2.equals("BLACK"))){
              color = color2
          }
      
          if (color1.equals("GRAY")  && color2.equals("BLACK")) {
                  color = color2
          }
      
          if (color2.equals("WHITE") &&  (color1.equals("GRAY") || color1.equals("BLACK"))) {
              color = color1
          }
      
          if (color2.equals("GRAY") && color1.equals("BLACK")) {
              color = color1
          }
      
          (edges, distance, color)
      }

      
      
      def bfsMap(node:(Int, (List[String], Int, String))):MutableList[(Int, (List[String], Int, String))] = {
        val (characterID, data) = node
        val (connections, distance, mycolor) = data
        var color = mycolor
        //If this node needs to be expanded...
        var results = new MutableList[(Int, (List[String], Int, String))]()
        val tmpColor = null
        if (color == "GRAY") {
            for (connection <- connections) {
                val newCharacterID = connection.toInt
                val newDistance = distance + 1
                val newColor = "GRAY"
                if (targetCharacterID.toString.equals(connection)) {
                    hitCounter.add(1)
                }
                val newEntry = (newCharacterID, (List[String](), newDistance, newColor))
                results += newEntry
            }
    
            //We've processed this node, so color it black
            color = "BLACK"
        }
    
        //Emit the input node so we don't lose it.
        val tpl:(Int, (List[String], Int, String)) = (characterID, (connections, distance, color))
        results +=  tpl
        results

      }
    println("Starting computation.....")
    val inputFile = sc.textFile("file:///vagrant/Marvel-Graph.txt")
    var iterationRdd = inputFile.map(convertToBFS)
    
    breakable {
      for (iteration <- 1 to 10) {
        println( "Running BFS iteration# " + (iteration+1).toString)
    
        //Create new vertices as needed to darken or reduce distances in the
        //reduce stage. If we encounter the node we're looking for as a GRAY
        //node, increment our accumulator to signal that we're done.
        val mapped = iterationRdd.flatMap(bfsMap)
    
        //Note that mapped.count() action here forces the RDD to be evaluated, and
        // that's the only reason our accumulator is actually updated.
        println( "Processing " + (mapped.count()) + " values.")
    
        if (hitCounter.value > 0) {
            println("**********************Hit the target character! From " + (hitCounter.value)  + 
                       " different direction(s).")
           break
        }
        iterationRdd = mapped.reduceByKey(bfsReduce)
        
      }
    }
  }
}