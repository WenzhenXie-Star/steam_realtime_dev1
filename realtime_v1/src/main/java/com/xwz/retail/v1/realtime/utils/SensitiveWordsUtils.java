package com.xwz.retail.v1.realtime.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @Package com.stream.utils.SensitiveWordsUtils
 * @Author zhou.han
 * @Date 2025/3/16 21:58
 * @description: sensitive words
 */
public class SensitiveWordsUtils {


    public static ArrayList<String> getSensitiveWordsLists(){
        ArrayList<String> res = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader("D:\\Zg3Work\\steam_realtime_dev\\realtime_v1\\src\\main\\resources\\Identify-sensitive-words.txt"))){
            String line ;
            while ((line = reader.readLine()) != null){
                res.add(line);
            }
        }catch (IOException ioException){
            ioException.printStackTrace();
        }
        return res;
    }

    public static <T> T getRandomElement(List<T> list) {
        if (list == null || list.isEmpty()) {
            return null;
        }
        Random random = new Random();
        int randomIndex = random.nextInt(list.size());
        return list.get(randomIndex);
    }

    public static void main(String[] args) {
        System.err.println(getRandomElement(getSensitiveWordsLists()));
    }
}
