package com.apple.bean

case class Girl(name: String, age: String, gender: String, school: School) {
  def showInfo(): String = {
    s"$name is $age years old"
  }
}

object Girl {
//  def showAge(age: Int): String = {
//    age + " years old"
//  }
}
