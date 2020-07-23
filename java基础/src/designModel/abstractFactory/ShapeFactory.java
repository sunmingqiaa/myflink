package designModel.abstractFactory;

public class ShapeFactory extends AbstactFactory{
    @Override
    public Color getColor(String color) {
        return null;
    }

    @Override
    public Shape getShape(String shapetype) {
        if (shapetype == "null") {
            return null;

        }else if (shapetype =="square") {
            return new Square();

        }else if (shapetype =="rectangle") {
            return new Rectangle();

        }
        return null;
    }
}
