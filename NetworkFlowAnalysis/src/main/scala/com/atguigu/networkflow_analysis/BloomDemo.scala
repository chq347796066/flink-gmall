package com.atguigu.networkflow_analysis

object BloomDemo {
  def main(args: Array[String]): Unit = {


  }

}

class BloomFilter(size: Long) extends Serializable{
  private val cap = size    // 默认cap应该是2的整次幂

  // hash函数
  def hash(value: String, seed: Int): Long = {
    var result = 0
    for( i <- 0 until value.length ){
      result = result * seed + value.charAt(i)
    }
    // 返回hash值，要映射到cap范围内
    (cap - 1) & result
  }
}
