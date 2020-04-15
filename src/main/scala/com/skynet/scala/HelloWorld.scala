package com.skynet.scala

object HelloWorld {
  def main(args: Array[String]){
    print("Hello\t World\n\n");
    
    println(getNothing(12));
    
    println(getNothing('c'));
    
    val nil : List[String] = Nil;
    
    println("List Empty or Not: " + nil.isEmpty);
    
    val seq = yieldWorks(5);
    
    val list : List[String] = List("Yuvi","Anna","Philip");
    
    list.flatMap(e => {
      e.toUpperCase()
      }).map(m => {
        println(m);
      });
    
    val yColl =  for( i <- list ) yield i.toUpperCase();
    
    val yT = for ( i <- list) yield {" Welcome " + i};
    
    yColl.map(e => {println(e)});
    
    yT.map(e => {println(e)});
  } 
  
  def getNothing(id : AnyVal) {
    
    if (id.isInstanceOf[Int]){
      println("The data type is integer");
    }
    
     if (id.isInstanceOf[Char]){
      println("The data type is Character");
    }
  }
  
  def getNull(id : Null) {
    
  }
  
  
  def yieldWorks(n: Int) : IndexedSeq[Int] = {
    
  var op = { for ( i <- 1 to n) yield i*2 };
    
    return op;
  }
  
}