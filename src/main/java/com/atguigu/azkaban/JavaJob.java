package com.atguigu.azkaban;

import java.io.FileOutputStream;
import java.io.IOException;

/**
 * @author clown
 */
public class JavaJob {
    public static void main(String[] args) {
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream("/opt/module/dates/java.txt");
            fos.write("you are the best !".getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                fos.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }
}
