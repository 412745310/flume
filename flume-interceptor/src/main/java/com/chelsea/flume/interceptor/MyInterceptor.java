package com.chelsea.flume.interceptor;

import java.util.List;

import org.apache.commons.codec.Charsets;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

/**
 * 自定义拦截器
 * 
 * @author shevchenko
 *
 */
public class MyInterceptor implements Interceptor {

    private String ip;

    public MyInterceptor(String ip) {
        this.ip = ip;
    }

    @Override
    public void close() {
    }

    @Override
    public void initialize() {
    }

    @Override
    public Event intercept(Event event) {
        // 获得body的内容
        String eventBody = new String(event.getBody(), Charsets.UTF_8);
        String fmt = "%s %s";
        // 添加ipAddress 到event的开头
        event.setBody(String.format(fmt, ip, eventBody).getBytes());
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        for (Event event : events) {
            intercept(event);
        }
        return events;
    }

    /**
     * 实现内部类接口
     */
    public static class AppendIPBuilder implements Interceptor.Builder {

        private String ip;

        @Override
        public void configure(Context context) {
            String ip = context.getString("ipAddress");
            this.ip = ip;
        }

        @Override
        public Interceptor build() {
            return new MyInterceptor(ip);
        }

    }

}
