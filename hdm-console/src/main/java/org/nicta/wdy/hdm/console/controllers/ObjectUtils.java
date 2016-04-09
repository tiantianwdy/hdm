package org.nicta.wdy.hdm.console.controllers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * Created by tiantian on 8/04/16.
 */
public class ObjectUtils {

    public static ObjectMapper mapper = new ObjectMapper();

    public static String objectToJson(Object obj) throws JsonProcessingException {

        String res = mapper.writeValueAsString(obj);
        return res;
    }

    public static <T> T  getObject(String jsonStr, Class<T> cls) throws IOException {
        T obj = mapper.readValue(jsonStr, cls);
        return obj;
    }
}
