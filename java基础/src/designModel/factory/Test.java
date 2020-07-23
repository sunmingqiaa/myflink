package designModel.factory;

public class Test {
    public static void main(String[] args) {
        Shape square = ShapeFactory.getShape("square");
        square.getShape();
        Shape rectangle = ShapeFactory.getShape("rectangle");
        rectangle.getShape();
    }
}
