package com.atguigu.dga.util;

import okhttp3.*;

import java.io.IOException;

public class HttpUtil {

    static OkHttpClient httpClient = new OkHttpClient();

    public static String get(String url) {
        //封装request对象
        Request request = new Request.Builder()
                .url(url)
                .get()
                .build();
        Call call = httpClient.newCall(request);
        try {
            Response response = call.execute();
            ResponseBody body = response.body();
            assert body != null;
            return body.string();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
