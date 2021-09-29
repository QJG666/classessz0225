package com.atguigu.bigdata.spark.test;

import java.util.ArrayList;

public class TestClone {
    public static void main(String[] args) {

        User user = new User();
        user.name = "zhangsan";

        ArrayList<User> users = new ArrayList<>();
        users.add(user);

        // 克隆的浅复制
        ArrayList<User> newUsers = (ArrayList<User>)users.clone();
        User newUser = newUsers.get(0);
        newUser.name = "lisi";

        System.out.println(users);
        System.out.println(newUsers);

    }
}
