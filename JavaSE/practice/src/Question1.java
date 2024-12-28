import java.util.ArrayList;

public class Question1 {
    public static void main(String[] args) {
        int startNum = 101;
        int endNum = 2000;
        ArrayList<Integer> primeFactor = getPrimeFactor(endNum);
        System.out.println(primeFactor);

        int[] nums = new int[endNum - startNum + 1];
        for (int i = startNum; i <= endNum; ++i) {
            nums[i - startNum] = i;
        }

        for (int num : primeFactor) {
            int left = startNum / num;
            int right = endNum / num;
            for (int j = left; j <= right; ++j) {
                if (num * j >= startNum && j >= 2) {
                    nums[num * j - startNum] = 0;
                }
            }
        }

        ArrayList<Integer> primeResult = new ArrayList<>();
        for (int num : nums) {
            if (num != 0) {
                primeResult.add(num);
            }
        }

        System.out.println(primeResult);
    }

    public static ArrayList<Integer> getPrimeFactor(int endNum) {
        // 获取素数因数
        ArrayList<Integer> factor = new ArrayList<>();
        for (int i = 2; i < Math.sqrt(endNum); ++i) {
            factor.add(i);
        }
        for (int i = 0; i < factor.size(); ++i) {
            if (factor.get(i) != 0 && (factor.get(i) * factor.get(i) <= factor.get(factor.size() - 1))) {
                for (int j = factor.get(i); j < factor.size(); ++j) {
                    int num = factor.get(i) * j;
                    if (num <= factor.get(factor.size() - 1)) {
                        factor.set(num - 2, 0);
                    } else {
                        break;
                    }
                }
            }
        }

        ArrayList<Integer> prime = new ArrayList<>();
        for (Integer integer : factor) {
            if (integer != 0) {
                prime.add(integer);
            }
        }
        return prime;
    }
}
