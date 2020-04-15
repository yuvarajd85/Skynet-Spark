package com.skynet.scala

import scala.collection.mutable._;

object AppMainExm extends App {
  
  println("Hello App Example");
  voidMethod();
  
  def voidMethod() : Unit = {
    val someList = List("Not","interested");
    someList.map(e => {println(e)});
    
    val listMut = ListBuffer[String]();
    listMut += "some";
    listMut += "Values";
    listMut += "are in Collection";
    listMut.map(e => println(e));
  }
}