package com.atguigu.hive;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class SplitWord extends GenericUDTF {

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        List<? extends StructField> inputFields = argOIs.getAllStructFieldRefs();
        if (inputFields.size() != 1) {
            throw new UDFArgumentException("The function ik_word_split() takes exactly one argument.");
        }
        if (!inputFields.get(0).getFieldObjectInspector().getCategory().equals(ObjectInspector.Category.PRIMITIVE)) {
            throw new UDFArgumentTypeException(1, "Arguments type must be primitive.");
        }
        ArrayList<String> fieldNames = new ArrayList<>();
        fieldNames.add("split_explode_col");
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<>();
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] objects) throws HiveException {
        String inputWord = objects[0].toString();
        List<String> wordList = getWordList(inputWord);
        for (String word : wordList) {
            forward(new Object[]{word});
        }
    }

    @Override
    public void close() throws HiveException {
    }

    public List<String> getWordList(String word) {
        List<String> keywordList = new ArrayList<>();

        // 创建IK分词器对象
        StringReader stringReader = new StringReader(word);
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
