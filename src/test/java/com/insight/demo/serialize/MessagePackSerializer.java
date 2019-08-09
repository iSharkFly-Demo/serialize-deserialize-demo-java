package com.insight.demo.serialize;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.insight.demo.serialize.model.msgpack.MessageData;
import com.insight.demo.utils.MockDataUtils;
import org.junit.Assert;
import org.junit.Test;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

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
