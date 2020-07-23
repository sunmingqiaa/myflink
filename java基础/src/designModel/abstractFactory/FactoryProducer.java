package designModel.abstractFactory;

public class FactoryProducer {
    public static AbstactFactory getFactory(String factory){
        if (factory =="shape") {
            return new ShapeFactory();

        }else if (factory =="color") {
            return new ColorFactory();

        }
        return null;

    }
}
