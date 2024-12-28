public class Question6 {
    public static void main(String[] args) {
        System.out.println(f(8));
    }

    public static int f(int n) {
        if (n == 1) {
            return 10;
        }
        return 2 + f(n - 1);
    }
}
