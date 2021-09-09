package com.yuhang.distributed_test

/**
 * @author yyh
 * @create 2021-05-19 9:17
 */
class Task extends Serializable{

    val datas = List(1,2,3,4)

    //val logic = ( num:Int )=>{ num * 2 }
    val logic : (Int)=>Int = _ * 2

}
