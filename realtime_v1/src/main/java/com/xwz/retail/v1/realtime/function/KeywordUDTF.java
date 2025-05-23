package com.xwz.retail.v1.realtime.function;

import com.xwz.retail.v1.realtime.utils.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @Package com.xwz.stream.realtime.v1.function.KeywordUDTF
 * @Author Wenzhen.Xie
 * @Date 2025/4/18 18:57
 * @description: KeywordUDTF
 */

@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class KeywordUDTF extends TableFunction<Row> {
    public void eval(String text) {
        for (String keyword : KeywordUtil.analyze(text)) {
            collect(Row.of(keyword));
        }
    }
}