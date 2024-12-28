package com.atguigu.flink.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Event {
    private String user;
    private String url;
    private Long ts;

    @Override
    public String toString() {
        return "user=" + user +
                "\turl=" + url +
                "\ttimestamp=" + ts;
    }
}
