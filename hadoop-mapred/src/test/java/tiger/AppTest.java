package tiger;

import java.util.Arrays;

/**
 * Unit test for simple App.
 */
public class AppTest {


    public static void main(String[] args) {
        String s="Unit test for,. simple App.";
        String[] split = s.split("[\\s+|,|.]");
        System.out.println(Arrays.toString(split));

        s="abc";
        System.out.println(s.matches("[a-z]+"));

    }


}
