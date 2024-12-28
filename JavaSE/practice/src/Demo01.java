import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Demo01 {
    public static void main(String[] args) {
        String sql = "#!/bin/bash\n" +
                "\nsql=\\\"\nset hive.mapjoin.optimized.hashtable=false;\n" +
                "insert overwrite table gmall.ads_sku_cart_num_top3_by_cate\n" +
                "select * from gmall.ads_sku_cart_num_top3_by_cate\n" +
                "union\n" +
                "select\n" +
                "    '${do_date}' dt,\n" +
                "    category1_id,\n" +
                "    category1_name,\n" +
                "    category2_id,\n" +
                "    category2_name,\n" +
                "    category3_id,\n" +
                "    category3_name,\n" +
                "    sku_id,\n" +
                "    sku_name,\n" +
                "    cart_num,\n" +
                "    rk\n" +
                "from\n" +
                "(\n" +
                "    select\n" +
                "        sku_id,\n" +
                "        sku_name,\n" +
                "        category1_id,\n" +
                "        category1_name,\n" +
                "        category2_id,\n" +
                "        category2_name,\n" +
                "        category3_id,\n" +
                "        category3_name,\n" +
                "        cart_num,\n" +
                "        rank() over (partition by category1_id,category2_id,category3_id order by cart_num desc) rk\n" +
                "    from\n" +
                "    (\n" +
                "        select\n" +
                "            sku_id,\n" +
                "            sum(sku_num) cart_num\n" +
                "        from gmall.dwd_trade_cart_full\n" +
                "        where dt='${do_date}'\n" +
                "        group by sku_id\n" +
                "    )cart\n" +
                "    left join\n" +
                "    (\n" +
                "        select\n" +
                "            id,\n" +
                "            sku_name,\n" +
                "            category1_id,\n" +
                "            category1_name,\n" +
                "            category2_id,\n" +
                "            category2_name,\n" +
                "            category3_id,\n" +
                "            category3_name\n" +
                "        from gmall.dim_sku_full\n" +
                "        where dt='${do_date}'\n" +
                "    )sku\n" +
                "    on cart.sku_id=sku.id\n" +
                ")t1\n" +
                "where rk<=3;\n" +
                "set hive.mapjoin.optimized.hashtable=true;\n" +
                "\\\"\n" +
                "\n" +
                "hive -e \\\"$sql\\\"";
        String regex = "((with.*)?insert.+?)[;\"]";
        Pattern compile = Pattern.compile(regex, Pattern.DOTALL);
        Matcher matcher = compile.matcher(sql);
        while (matcher.find()) {
            System.out.print(matcher.group(1).trim());
        }
    }
}