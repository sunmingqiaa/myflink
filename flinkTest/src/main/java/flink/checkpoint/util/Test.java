package flink.checkpoint.util;

public class Test {
    public static void main(String[] args) {
        System.out.println( PropertiesUtil.getvalue("bootstrap"));
        System.out.println( PropertiesUtil.getvalue("zookeeper.connect"));
        System.out.println( PropertiesUtil.getvalue("hh"));
        System.out.println( PropertiesUtil.getvalue("topic"));
    }
}
