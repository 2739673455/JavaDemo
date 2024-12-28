package com.atguigu;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Exercise8 {
    public static void main(String[] args) {
        String str = "'北京': ['北京'];"
                + "'上海': ['上海'];"
                + "'天津': ['天津'];"
                + "'重庆': ['重庆'];"
                + "'河北省': ['石家庄', '张家口', '承德', '秦皇岛', '唐山', '廊坊', '保定', '沧州', '衡水', '邢台', '邯郸'];"
                + "'山西省': ['太原', '大同', '朔州', '阳泉', '长治', '晋城', '忻州', '吕梁', '晋中', '临汾', '运城'];"
                + "'辽宁省': ['沈阳', '朝阳', '阜新', '铁岭', '抚顺', '本溪', '辽阳', '鞍山', '丹东', '大连', '营口', '盘锦', '锦州', '葫芦岛'];"
                + "'吉林省': ['长春', '白城', '松原', '吉林', '四平', '辽源', '通化', '白山', '延边'];"
                + "'黑龙江省': ['哈尔滨', '齐齐哈尔', '黑河', '大庆', '伊春', '鹤岗', '佳木斯', '双鸭山', '七台河', '鸡西', '牡丹江', '绥化', '大兴安'];"
                + "'江苏省': ['南京', '徐州', '连云港', '宿迁', '淮阴', '盐城', '扬州', '泰州', '南通', '镇江', '常州', '无锡', '苏州'];"
                + "'浙江省': ['杭州', '湖州', '嘉兴', '舟山', '宁波', '绍兴', '金华', '台州', '温州', '丽水','衢州'];"
                + "'安徽省': ['合肥', '宿州', '淮北', '阜阳', '蚌埠', '淮南', '滁州', '马鞍山', '芜湖', '铜陵', '安庆', '黄山', '六安', '巢湖', '池州', '宣城'];"
                + "'福建省': ['福州', '南平', '三明', '莆田', '泉州', '厦门', '漳州', '龙岩', '宁德'];"
                + "'江西省': ['南昌', '九江', '景德镇', '鹰潭', '新余', '萍乡', '赣州', '上饶', '抚州', '宜春', '吉安'];"
                + "'山东省': ['济南', '聊城', '德州', '东营', '淄博', '潍坊', '烟台', '威海', '青岛', '日照', '临沂', '枣庄', '济宁', '泰安', '莱芜', '滨州', '菏泽'];"
                + "'河南省': ['郑州', '三门峡', '洛阳', '焦作', '新乡', '鹤壁', '安阳', '濮阳', '开封', '商丘', '许昌', '漯河', '平顶山', '南阳', '信阳', '周口', '驻马店'];"
                + "'湖北省': ['武汉', '十堰', '襄攀', '荆门', '孝感', '黄冈', '鄂州', '黄石', '咸宁', '荆州', '宜昌', '恩施', '襄樊'];"
                + "'湖南省': ['长沙', '张家界', '常德', '益阳', '岳阳', '株洲', '湘潭', '衡阳', '郴州', '永州', '邵阳', '怀化', '娄底', '湘西'];"
                + "'广东省': ['广州', '清远', '韶关', '河源', '梅州', '潮州', '汕头', '揭阳', '汕尾', '惠州', '东莞', '深圳', '珠海', '江门', '佛山', '肇庆', '云浮', '阳江', '茂名', '湛江'];"
                + "'海南省': ['海口', '三亚'];"
                + "'四川省': ['成都', '广元', '绵阳', '德阳', '南充', '广安', '遂宁', '内江', '乐山', '自贡', '泸州', '宜宾', '攀枝花', '巴中', '达川', '资阳', '眉山', '雅安', '阿坝', '甘孜', '凉山'];"
                + "'贵州省': ['贵阳', '六盘水', '遵义', '毕节', '铜仁', '安顺', '黔东南', '黔南', '黔西南'];"
                + "'云南省': ['昆明', '曲靖', '玉溪', '丽江', '昭通', '思茅', '临沧', '保山', '德宏', '怒江', '迪庆', '大理', '楚雄', '红河', '文山', '西双版纳'];"
                + "'陕西省': ['西安', '延安', '铜川', '渭南', '咸阳', '宝鸡', '汉中', '榆林', '商洛', '安康'];"
                + "'甘肃省': ['兰州', '嘉峪关', '金昌', '白银', '天水', '酒泉', '张掖', '武威', '庆阳', '平凉', '定西', '陇南', '临夏', '甘南'];"
                + "'青海省': ['西宁', '海东', '西宁', '海北', '海南', '黄南', '果洛', '玉树', '海西'];"
                + "'内蒙古': ['呼和浩特', '包头', '乌海', '赤峰', '呼伦贝尔盟', '兴安盟', '哲里木盟', '锡林郭勒盟', '乌兰察布盟', '鄂尔多斯', '巴彦淖尔盟', '阿拉善盟'];"
                + "'广西': ['南宁', '桂林', '柳州', '梧州', '贵港', '玉林', '钦州', '北海', '防城港', '南宁', '百色', '河池', '柳州', '贺州'];"
                + "'西藏': ['拉萨', '那曲', '昌都', '林芝', '山南', '日喀则', '阿里'];"
                + "'宁夏': ['银川', '石嘴山', '吴忠', '固原'];"
                + "'新疆': ['乌鲁木齐', '克拉玛依', '喀什', '阿克苏', '和田', '吐鲁番', '哈密', '博尔塔拉', '昌吉', '巴音郭楞', '伊犁', '塔城', '阿勒泰'];"
                + "'香港': ['香港'];"
                + "'澳门': ['澳门'];"
                + "'台湾': ['台北', '台南', '其他']";

//        answer1(str);
        answer2(str);

    }

    public static void answer1(String str) {
        HashMap<String, ArrayList<String>> map = new HashMap<>();
        String[] strs1 = str.split(";");
        for (String s1 : strs1) {
            String[] strs2 = s1.split(":");
            strs2[1] = strs2[1].trim();
            strs2[1] = strs2[1].substring(1, strs2[1].length() - 1);
            String[] strs3 = strs2[1].split(",");
            ArrayList<String> list = new ArrayList<>(Arrays.asList(strs3));
            for (int i = 0; i < list.size(); ++i) {
                String s2 = list.get(i);
                s2 = s2.trim();
                s2 = s2.substring(1, s2.length() - 1);
                list.set(i, s2);
            }
            map.put(strs2[0].substring(1, strs2[0].length() - 1), list);
        }

        getCity(map);
    }

    public static void answer2(String str) {
        HashMap<String, ArrayList<String>> map = new HashMap<>();
        String[] strs1 = str.split(";");
        for (String s1 : strs1) {
            String[] pairs = s1.split(":");
            //定义正则表达式
            String regex = "'(.*?)'";
            Pattern pattern = Pattern.compile(regex);
            //取省份
            String province = "";
            Matcher matcher = pattern.matcher(pairs[0]);
            while (matcher.find()) {
                province = matcher.group(1);
            }
            //取城市
            ArrayList<String> city = new ArrayList<>();
            matcher = pattern.matcher(pairs[1]);
            while (matcher.find()) {
                city.add(matcher.group(1));
            }
            System.out.println(city);
            map.put(province, city);
        }

//        getCity(map);
    }

    public static void getCity(HashMap<String, ArrayList<String>> map) {
        Scanner input = new Scanner(System.in);
        for (String s : map.keySet()) {
            System.out.println(s);
        }
        System.out.print("请输入省份:");
        System.out.println(map.get(input.next()));
        input.close();
    }
}
