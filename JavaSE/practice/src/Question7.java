public class Question7 {
    public static void main(String[] args) {
        System.out.println(f(8));
    }

    public static int f(int n) {
        if (n == 0) {
            return 1;
        } else if (n < 0) {
            return 0;
        }
        return f(n - 1) + f(n - 2);
    }
}
