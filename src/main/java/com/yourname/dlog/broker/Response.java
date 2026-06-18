package com.yourname.dlog.broker;

import java.util.List;

import com.yourname.dlog.log.Record;

public class Response {
    public boolean success;
    public String error;
    public long offset;           // result of APPEND
    public List<Record> records;  // result of FETCH

    public Response() {}

    public static Response ok() {
        Response r = new Response();
        r.success = true;
        return r;
    }

    public static Response error(String message) {
        Response r = new Response();
        r.success = false;
        r.error = message;
        return r;
    }
} 
