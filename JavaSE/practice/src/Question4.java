public class Question4 {
    public static void main(String[] args) {
        System.out.println(f(20));
    }

    public static long f(int n) {
        if (n == 1) {
            return 1;
        }
        return n + f(n - 1);
    }
}
