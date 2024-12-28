public class Question8 {
    public static void main(String[] args) {
        int num = 98765;
        int result = 0;
        while (num != 0) {
            result = (num % 10) + result * 10;
            num /= 10;
        }
        System.out.println(result);
    }
}
