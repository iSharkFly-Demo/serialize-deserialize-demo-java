package com.insight.demo.serialize;

import com.insight.demo.serialize.model.msgpack.DataCacheMessage;
import org.junit.Test;

import org.msgpack.MessagePack;
import org.msgpack.packer.Packer;
import org.msgpack.template.ListTemplate;
import org.msgpack.template.Templates;
import org.msgpack.type.ArrayValue;
import org.msgpack.type.Value;
import org.msgpack.unpacker.Converter;
import org.msgpack.unpacker.Unpacker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MessagePackDataAnalysis {

    final Logger logger = LoggerFactory.getLogger(MessagePackDataAnalysis.class);

    @Test
    public void testMessagePackSerialization() {

        DataCacheMessage dataCacheMessage1 = new DataCacheMessage();
        dataCacheMessage1.compact = true;
        dataCacheMessage1.schema=0;

        DataCacheMessage dataCacheMessage2 = new DataCacheMessage();
        dataCacheMessage2.compact = false;
        dataCacheMessage2.schema=0;



        List<DataCacheMessage> src = new ArrayList<>();
        src.add(dataCacheMessage1);
        src.add(dataCacheMessage2);


        MessagePack msgpack = new MessagePack();
// Serialize

        try {
            byte[] raw = msgpack.write(src);
            byte[] rawMsg = msgpack.write(dataCacheMessage1);

            logger.debug("{}",raw);

//

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            Packer packer = msgpack.createPacker(out);
            packer.write(dataCacheMessage1);
            packer.write(dataCacheMessage2);
            byte[] bytes = out.toByteArray();

            ByteArrayInputStream in = new ByteArrayInputStream(bytes);
            Unpacker unpacker = msgpack.createUnpacker(in);
            DataCacheMessage dst1 = unpacker.read(DataCacheMessage.class);
            DataCacheMessage dst2 = unpacker.read(DataCacheMessage.class);
            logger.debug("{}",dst1.compact);
            logger.debug("{}",dst2.compact);




//            DataCacheMessage dst = msgpack.read(rawMsg, DataCacheMessage.class);
//            logger.debug("{}",dst.compact);
//
//
//            DataCacheMessage dst1 = msgpack.read(raw, DataCacheMessage.class);
//
//            logger.debug("{}",dst1);

//            rawMsg


//            msgpack.register(List.class, new ListTemplate(new DataCacheMessage()));

            Value value = msgpack.read(raw);

            if(value.isArrayValue()) {
                DataCacheMessage[] array = msgpack.convert(value, new DataCacheMessage[1000000]);

                System.out.println(array.length);
                System.out.println(array[0].compact);
                System.out.println(array[1].compact);


            }

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
