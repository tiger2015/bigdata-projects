package com.tiger.java;

/**
 * @Author Zenghu
 * @Date 2021/3/17 22:56
 * @Description
 * @Version: 1.0
 **/
public class JavaOverrideTest {

    public static void main(String[] args) {
        Super s1 = new Sub();
        Sub s2 = new Sub();
        System.out.println(s1.s); // super
        System.out.println(s2.s); // sub
    }

    static class Super {
        String s = "super";
    }

    static class Sub extends Super {
        String s = "sub";
    }
}



