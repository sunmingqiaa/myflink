package designModel.abstractFactory;

public class Test {
    public static void main(String[] args) {
        AbstactFactory colorFactory = FactoryProducer.getFactory("color");
        Color red = colorFactory.getColor("red");
        red.getColor();
    }
}
