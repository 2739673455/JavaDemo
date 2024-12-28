package com.atguigu.flink.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class UrlCount {
    private Long startTime;
    private Long endTime;
    private String url;
    private Long count;
    private Long ts;

    @Override
    public String toString() {
        return startTime + "\t" + endTime + "\t" + url + ":" + count + "  \tts=" + ts;
    }
}
