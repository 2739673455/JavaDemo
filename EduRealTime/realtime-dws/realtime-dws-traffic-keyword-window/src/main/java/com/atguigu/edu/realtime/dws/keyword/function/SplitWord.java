package com.atguigu.edu.realtime.dws.keyword.function;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

@FunctionHint(output = @DataTypeHint("ROW<keyword string>"))
public class SplitWord extends TableFunction<Row> {
    public void eval(String keyword) {
        analyze(keyword).forEach(word -> collect(Row.of(word)));
    }

    public static List<String> analyze(String text) {
        List<String> keywordList = new ArrayList<String>();
        // 创建IK分词器对象
        StringReader stringReader = new StringReader(text);
        IKSegmenter ik = new IKSegmenter(stringReader, true);
        Lexeme lexeme;
        try {
            while ((lexeme = ik.next()) != null) {
                String keyword = lexeme.getLexemeText();
                keywordList.add(keyword);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return keywordList;
    }
}