public class Question3 {
    public static void main(String[] args) {
//        1 0 -1.5 -100
//        0 1  2.5  200
//        x - 1.5z = -100
//        y + 2.5z = 200
//        x=1.5z-100
//        y=200-2.5z
        for (int z = 1; z <= 100 * 2; ++z) {
            double x = 1.5 * z - 100;
            double y = 200 - 2.5 * z;
            if (x % 1 == 0 && y % 1 == 0 && x >= 0 && y >= 0) {
                System.out.println("x=" + x + " y=" + y + " z=" + z);
            }
        }
    }
}
