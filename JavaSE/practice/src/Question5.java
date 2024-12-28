public class Question5 {
    public static void main(String[] args) {
        long sum = 1;
        long m = 1;
        for (int i = 2; i <= 20; ++i) {
            m *= i;
            sum += m;
        }
        System.out.println(sum);
    }
}
