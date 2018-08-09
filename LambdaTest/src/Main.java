public class Main{
    public interface Inter {
        void printIt(String text);
    }
    public static void main(String[] args){
        Inter inter = (String text) -> System.out.println(text);
        inter.printIt("oi");

    }
}
