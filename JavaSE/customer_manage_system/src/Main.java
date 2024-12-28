import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        Scanner input = new Scanner(System.in);

//        String str1 = CMUtility.readString(5);
        String str2 = input.nextLine();
        System.out.println(str2.isEmpty());
//        System.out.println(str1.length());
        input.close();
    }
}
