package tiger;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import java.util.Scanner;

/**
 * Unit test for simple App.
 */
public class AppTest {
    public static void main(String[] args) {

        Integer i = null;

        System.out.println(i instanceof Object);

        short s1 = 1;

//        Scanner scanner = new Scanner(System.in);
//        int b = scanner.nextInt();
//        System.out.println(b);

        String str1 = "abc";//
        String str2 = "abc";
        System.out.println(str1 == str2);

    }

    static void change(int a) {
        a = a * 10;
    }
}
