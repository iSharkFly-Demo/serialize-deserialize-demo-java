package com.insight.demo.serialize;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.insight.demo.serialize.model.msgpack.MessageData;
import com.insight.demo.utils.MockDataUtils;
import org.junit.Assert;
import org.junit.Test;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.msgpack.jackson.dataformat.MessagePackKeySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

import static org.junit.Assert.assertEquals;

public class MessagePackSerializer {

    final Logger logger = LoggerFactory.getLogger(MessagePackSerializer.class);

    /**
     * SerializationPOJO
     */
    @Test
    public void testMessagePackSerializationPOJO() {

        byte[] bytes = new byte[0];
        String uuid = UUID.randomUUID().toString();

        // Instantiate ObjectMapper for MessagePack
        ObjectMapper objectMapper = new ObjectMapper(new MessagePackFactory());


        MessageData pojo = new MessageData();
        pojo.setUuid(uuid);
        pojo.setName("CWIKI.US");


        try {
            // Serialize a Java object to byte array
            bytes = objectMapper.writeValueAsBytes(pojo);
            logger.debug("Length of Bytes: [{}]", bytes.length);

            // Deserialize the byte array to a Java object
            MessageData deserialized = objectMapper.readValue(bytes, MessageData.class);
            logger.debug("Deserialized Name: [{}]", deserialized.name);

            assertEquals("CWIKI.US", deserialized.name);

        } catch (JsonProcessingException ex) {
            logger.error("Serialize Error", ex);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * SerializationList
     */
    @Test
    public void testMessagePackSerializationList() {

        byte[] bytes = new byte[0];
        String uuid = UUID.randomUUID().toString();

        // Instantiate ObjectMapper for MessagePack
        ObjectMapper objectMapper = new ObjectMapper(new MessagePackFactory());

        List<MessageData> objList = MockDataUtils.getMessageDataList(9);

        try {
            // Serialize a Java object to byte array
            bytes = objectMapper.writeValueAsBytes(objList);
            logger.debug("Length of Bytes: [{}]", bytes.length);

            // Deserialize the byte array to a Java object
            // Deserialize the byte array to a List
            List<MessageData> deserialized = objectMapper.readValue(bytes, new TypeReference<List<MessageData>>() {
            });
            logger.debug("Deserialized List Count: [{}]", deserialized.size());
            logger.debug("List index 0: [{}]", deserialized.get(0).name);

        } catch (JsonProcessingException ex) {
            logger.error("Serialize Error", ex);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * SerializationMap
     */
    @Test
    public void testMessagePackSerializationMap() {

        byte[] bytes = new byte[0];
        String uuid_a = UUID.randomUUID().toString();
        String uuid_b = UUID.randomUUID().toString();

        // Instantiate ObjectMapper for MessagePack
        ObjectMapper objectMapper = new ObjectMapper(new MessagePackFactory());

        Map<String, MessageData> map = new HashMap<>();
        MessageData messageData = new MessageData();

        // Element A in MAP
        messageData.setUuid(UUID.randomUUID().toString());
        messageData.setName("CWIKI.US - A");
        map.put(uuid_a, messageData);

        // Element B in MAP
        messageData = new MessageData();
        messageData.setUuid(UUID.randomUUID().toString());
        messageData.setName("CWIKI.US - B");
        map.put(uuid_b, messageData);


        try {
            // Serialize a Java object to byte array
            bytes = objectMapper.writeValueAsBytes(map);
            logger.debug("Length of Bytes: [{}]", bytes.length);

            // Deserialize the byte array to a MAP
            Map<String, MessageData> deserialized = objectMapper.readValue(bytes, new TypeReference<Map<String, MessageData>>() {
            });
            logger.debug("Deserialized MAP Count: [{}]", deserialized.size());
            logger.debug("MAP index 0: [{}]", deserialized.get(uuid_a).getName());

            assertEquals("CWIKI.US - A", deserialized.get(uuid_a).getName());

        } catch (JsonProcessingException ex) {
            logger.error("Serialize Error", ex);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * Serialization Not Close output stream
     */
    @Test
    public void testMessagePackSerializationNotCloseOutputStream() {
        logger.debug("testMessagePackSerializationNotCloseOutputStream");

        try {
            File tempFile = File.createTempFile("messagepack-", "-cwiki.us");

            OutputStream out = new FileOutputStream(tempFile);
            ObjectMapper objectMapper = new ObjectMapper(new MessagePackFactory());
            objectMapper.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);

            objectMapper.writeValue(out, 1);
            objectMapper.writeValue(out, "two");
            objectMapper.writeValue(out, 3.14);
            out.close();

            MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(new FileInputStream(tempFile));
            System.out.println(unpacker.unpackInt());      // => 1
            System.out.println(unpacker.unpackString());   // => two
            System.out.println(unpacker.unpackFloat());    // => 3.14

            tempFile.deleteOnExit();
        } catch (IOException ex) {
            logger.error("Serialize Error", ex);
        }
    }

    /**
     * Serialization Not Close input stream
     */
    @Test
    public void testMessagePackSerializationNotCloseInputStream() {
        logger.debug("testMessagePackSerializationNotCloseInputStream");

        try {
            File tempFile = File.createTempFile("messagepack-", "-cwiki.us");

            MessagePacker packer = MessagePack.newDefaultPacker(new FileOutputStream(tempFile));
            packer.packInt(42);
            packer.packString("Hello");
            packer.close();

            FileInputStream in = new FileInputStream(tempFile);
            ObjectMapper objectMapper = new ObjectMapper(new MessagePackFactory());
            objectMapper.configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, false);
            System.out.println(objectMapper.readValue(in, Integer.class));
            System.out.println(objectMapper.readValue(in, String.class));
            in.close();

            tempFile.deleteOnExit();
        } catch (IOException ex) {
            logger.error("Serialize Error", ex);
        }
    }

    /**
     * testMessagePackSerializationMapKey
     */
    @Test
    @JsonSerialize(keyUsing = MessagePackKeySerializer.class)
    public void testMessagePackSerializationMapKey() {
        logger.debug("testMessagePackSerializationNotCloseInputStream");

        byte[] bytes = new byte[0];
        Integer uuid_a = 101;
        Integer uuid_b = 102;

        // Instantiate ObjectMapper for MessagePack
        ObjectMapper objectMapper = new ObjectMapper(new MessagePackFactory());

        Map<Integer, MessageData> map = new HashMap<>();
        MessageData messageData = new MessageData();

        // Element A in MAP
        messageData.setUuid(UUID.randomUUID().toString());
        messageData.setName("CWIKI.US - A");
        map.put(uuid_a, messageData);

        // Element B in MAP
        messageData = new MessageData();
        messageData.setUuid(UUID.randomUUID().toString());
        messageData.setName("CWIKI.US - B");
        map.put(uuid_b, messageData);


        try {
            // Serialize a Java object to byte array
            bytes = objectMapper.writeValueAsBytes(map);
            logger.debug("Length of Bytes: [{}]", bytes.length);

            // Deserialize the byte array to a MAP
            Map<String, MessageData> deserialized = objectMapper.readValue(bytes, new TypeReference<Map<Integer, MessageData>>() {
            });
            logger.debug("Deserialized MAP Count: [{}]", deserialized.size());
            logger.debug("MAP index 0: [{}]", deserialized.get(uuid_a).getName());

            assertEquals("CWIKI.US - A", deserialized.get(uuid_a).getName());

        } catch (JsonProcessingException ex) {
            logger.error("Serialize Error", ex);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void testMessagePackSerialization() {
        ObjectMapper objectMapper = new ObjectMapper(new MessagePackFactory());

        MessageData messageData1 = new MessageData();
        messageData1.name = "CWIKI.US - A";
        messageData1.schema = 0;

        MessageData messageData2 = new MessageData();
        messageData1.name = "CWIKI.US - B";
        messageData2.schema = 0;


        List<MessageData> src = new ArrayList<>();
        src.add(messageData1);
        src.add(messageData2);


//        MessagePack msgpack = new MessagePack();
// Serialize

        try {

            byte[] raw = objectMapper.writeValueAsBytes(MockDataUtils.getMessageDataList(2));


            logger.debug("{}", raw);

//

//            ByteArrayOutputStream out = new ByteArrayOutputStream();
//            Packer packer = msgpack.createPacker(out);
//            packer.write(messageData1);
//            packer.write(messageData2);
//            byte[] bytes = out.toByteArray();
//
//            ByteArrayInputStream in = new ByteArrayInputStream(bytes);
//            Unpacker unpacker = msgpack.createUnpacker(in);
//            MessageData dst1 = unpacker.read(MessageData.class);
//            MessageData dst2 = unpacker.read(MessageData.class);
//            logger.debug("{}",dst1.compact);
//            logger.debug("{}",dst2.compact);


//            MessageData dst = msgpack.read(rawMsg, MessageData.class);
//            logger.debug("{}",dst.compact);
//
//
//            MessageData dst1 = msgpack.read(raw, MessageData.class);
//
//            logger.debug("{}",dst1);

//            rawMsg


//            msgpack.register(List.class, new ListTemplate(new MessageData()));

//            Value value = msgpack.read(raw);
//
//            if(value.isArrayValue()) {
//                MessageData[] array = msgpack.convert(value, new MessageData[1000000]);
//
//                System.out.println(array.length);
//                System.out.println(array[0].compact);
//                System.out.println(array[1].compact);
//
//
//            }

//            if(value.isRawValue()) {
//                System.out.println("sddddddddss");
//            }


//            List<byte[]> dst1 = msgpack.read(raw, new List());
//            System.out.println(dst1.get(0).length);
//            System.out.println(dst1.get(1).length);
////            System.out.println(dst1.get(1));
////            System.out.println(dst1.get(2));

        } catch (IOException e) {
            e.printStackTrace();
        }


//// Or, Deserialze to Value then convert type.
//        Value dynamic = msgpack.read(raw);
//        List<String> dst2 = new Converter(dynamic)
//                .read(Templates.tList(Templates.TString));
//        System.out.println(dst2.get(0));
//        System.out.println(dst2.get(1));
//        System.out.println(dst2.get(2));
//
//    }
    }
}
