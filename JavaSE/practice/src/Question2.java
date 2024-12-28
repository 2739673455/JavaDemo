public class Question2 {
    public static void main(String[] args) {
        double height = 100;
        double length = height;
        for (int i = 1; i <= 10; ++i) {
            System.out.println("第" + i + "次落地走过总路程:" + length);
            length += height;
            height /= 2;
            System.out.println("第" + i + "次反弹高度:" + height);
        }
    }
}
