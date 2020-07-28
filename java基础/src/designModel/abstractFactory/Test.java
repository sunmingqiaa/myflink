package designModel.abstractFactory;
/*
  *
  *@author sunmingqi
  *@date 2020/7/24
  */
public class Test {
    /*
      *
      *
      *@params [args]
      *@return void
      */
    public static void main(String[] args) {
        AbstactFactory colorFactory = FactoryProducer.getFactory("color");
        Color red = colorFactory.getColor("red");
        red.getColor();
    }
}
