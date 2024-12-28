package com.atguigu.dga.util;

import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;

import java.util.Collections;

public class SqlUtil {
    public static String filterUnsafeSql(String input) {
        if (input == null) {
            return null;
        }
        // 替换 MySQL 中可能导致 SQL 注入的特殊字符
        return input.replace("\\", "\\\\")
                .replace("'", "\\'")
                .replace("\"", "\\\"")
                .replace("\b", "\\b")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t")
                .replace("\u001A", "\\Z")
                .replace("%", "\\%")
                .replace("_", "\\_");
    }

    //将sql解析为语法树，对树遍历
    public static void parseSql(String sql, Dispatcher dispatcher) {
        //使用hive的解析工具
        try {
            ParseDriver parseDriver = new ParseDriver();
            ASTNode astNode = parseDriver.parse(sql);

            //将TOK_QUERY作为根节点
            while (astNode.getType() != HiveParser.TOK_QUERY) {
                //取第一个子节点
                astNode = (ASTNode) astNode.getChild(0);
            }

            //遍历树
            DefaultGraphWalker graphWalker = new DefaultGraphWalker(dispatcher);
            graphWalker.startWalking(Collections.singleton(astNode), null);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}