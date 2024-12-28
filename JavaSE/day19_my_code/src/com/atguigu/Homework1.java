package com.atguigu;

import java.util.ArrayList;
import java.util.Random;

public class Homework1 {
    public static void main(String[] args) {
        String[] nums = {"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "J", "Q", "K"};
        String[] colors = {"♠", "♦", "♥", "♣"};
        ArrayList<String> cards = new ArrayList<String>();
        Random random = new Random();
        for (int i = 0; i < nums.length; ++i) {
            for (int j = 0; j < colors.length; ++j) {
                cards.add(nums[i] + colors[j]);
            }
        }
        cards.add("大王");
        cards.add("小王");

        ArrayList<String>[] personCards = new ArrayList[4];
        for (int i = 0; i < 4; ++i) {
            personCards[i] = new ArrayList<>();
        }

        for (int i = 0; i < 4; ++i) {
            for (int j = 0; j < 11; ++j) {
                int index = random.nextInt(cards.size());
                personCards[i].add(cards.get(index));
                cards.remove(index);
            }
        }

        for (int i = 0; i < 4; ++i) {
            System.out.println(personCards[i]);
        }

        System.out.println(cards);
    }
}
