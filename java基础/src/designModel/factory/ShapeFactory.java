package designModel.factory;

public class ShapeFactory {
    public static Shape getShape(String shape){
        if (shape == "square") {
            return new Square();

        }else if (shape == "rectangle") {
            return new Rectangle();

        }
        return null;
    }

}
