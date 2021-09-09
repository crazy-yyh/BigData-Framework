package com.yuhang.acc;

import java.lang.reflect.Field;

/**
 * @author yyh
 * @create 2021-09-03 14:31
 */
public class R {

    private String username;

    public Integer age;


    public Integer getinfo(){
        return age;
    }

    public static void main(String[] args) throws NoSuchFieldException, IllegalAccessException {
        R rusult = new R();

        rusult.setUsername("yyh");
        rusult.setAge(30);


//        Field declaredField = rusult.getClass().getDeclaredField("username");
//
//        Object o = declaredField.get(rusult);
//
//        System.out.println(o.toString());
//
//        String str = declaredField.toString();
//
//        System.out.println(str);

        Field[] fi = rusult.getClass().getDeclaredFields();

        for (Field field : fi){

            System.out.println(field.getName());

            Object o = field.get(rusult);
            System.out.println(o);
        }
    }


    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }
}



