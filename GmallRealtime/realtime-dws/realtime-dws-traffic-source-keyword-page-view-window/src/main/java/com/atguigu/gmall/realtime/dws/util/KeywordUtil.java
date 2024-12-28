package com.atguigu.gmall.realtime.dws.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class KeywordUtil {
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
