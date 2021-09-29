package com.atguigu.bigdata.java;

public class TestEq {
    public static void main(String[] args) {
        User11 user1 = new User11();
        user1.id = 1;
        User11 user2 = new User11();
        user2.id = 1;

        System.out.println(user1.equals(user2));

//        user2.getClass();
//        User11.class
    }
}
class User11 {
    public Integer id;
    @Override
    public boolean equals(Object o) {
//        if ( o == null ) {
//            return false;
//        }
        if ( o instanceof User11 ) {
            User11 other = (User11)o;
            return other.id.equals(null);
        } else {
            return false;
        }
    }
}
