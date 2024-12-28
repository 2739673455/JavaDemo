package com.atguigu.test;

import com.atguigu.gmall.realtime.dws.util.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

@FunctionHint(output = @DataTypeHint("ROW<keyword string>"))
public class SplitWord extends TableFunction<Row> {
    public void eval(String keyword) {
        KeywordUtil.analyze(keyword).forEach(word -> collect(Row.of(word)));
    }
}
