package com.insight.demo.serialize;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.insight.demo.serialize.model.msgpack.MessageData;
import com.insight.demo.utils.MockDataUtils;
import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.msgpack.jackson.dataformat.JsonArrayFormat;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import java.util.List;

/**
 * MessagePackDataTest
 *
 * @author yhu
 */
public class MessagePackDataTest {

    final Logger logger = LoggerFactory.getLogger(MessagePackDataTest.class);

    /**
     * testDataSize
     */
    @Test
    public void testDataSize() {
        logger.debug("Test Data Size");

        byte[] raw;
        byte[] rawJsonArray;

        List<MessageData> dataList = MockDataUtils.getMessageDataList(600000);

        try {
            ObjectMapper objectMapper = new ObjectMapper(new MessagePackFactory());
            raw = objectMapper.writeValueAsBytes(dataList);

            FileUtils.byteCountToDisplaySize(raw.length);
            logger.debug("Raw Size: [{}]", FileUtils.byteCountToDisplaySize(raw.length));


            objectMapper = new ObjectMapper(new MessagePackFactory());
            objectMapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
            objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
            objectMapper.setAnnotationIntrospector(new JsonArrayFormat());

            rawJsonArray = objectMapper.writeValueAsBytes(dataList);
            logger.debug("rawJsonArray Size: [{}]", FileUtils.byteCountToDisplaySize(rawJsonArray.length));

            logger.debug("RawrawJsonArray Snappy Size: [{}]", FileUtils.byteCountToDisplaySize(Snappy.compress(rawJsonArray).length));

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
