import java.util.HashMap;

public class Question9 {
    public static void main(String[] args) {
        String content = "中中国55kkfff";
        HashMap<Character, Integer> map = new HashMap<>();
        for (int i = 0; i < content.length(); ++i) {
            if (map.get(content.charAt(i)) == null) {
                map.put(content.charAt(i), 1);
            } else {
                map.put(content.charAt(i), map.get(content.charAt(i)) + 1);
            }
        }
        System.out.println(map);
    }
}
