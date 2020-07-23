package designModel.abstractFactory;

public class ColorFactory extends AbstactFactory {

    @Override
    public Color getColor(String color) {
        if (color == null) {
            return null;

        }else if (color =="yellow") {
            return new Yellow();

        }else if (color =="red") {
            return new Red();

        }return null;
    }

    @Override
    public Shape getShape(String shape) {
        return null;
    }
}
