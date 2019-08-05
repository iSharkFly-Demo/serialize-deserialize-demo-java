package com.insight.demo.serialize;

import com.google.protobuf.ByteString;
import com.insight.demo.serialize.model.Tot.TotMessage;
import com.insight.demo.serialize.model.TotList.TotListMessage;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.joda.time.DateTime;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class ProtobufDataAnalysis {

    final Logger logger = LoggerFactory.getLogger(ProtobufDataAnalysis.class);


    @Test
    public void testTotCache() {

        long sTime = DateTime.now().getMillis();

        TotMessage.Builder builder = TotMessage.newBuilder();
        TotListMessage.Builder builderList = TotListMessage.newBuilder();


        for (int i = 0; i < 30000; i++) {
            // simple fields
            builder.setPortfolioId(RandomUtils.nextDouble(12, 2000))
                    .setTradableEntityId(RandomUtils.nextDouble(12, 2000))
                    .setQuantity(RandomUtils.nextDouble(0, 2000000))
                    .setTestPrice((RandomUtils.nextDouble(0, 2000000)))
                    .setTotalMarketValueUSDAmount(builder.getQuantity() * builder.getTestPrice())
                    .setRuleApplicationTimeCode("BOD")
                    .setTradeSide("BUY")
                    .setPositionCode("LONG")
                    .setHoldingTransactionId(RandomUtils.nextDouble(12, 2000))
                    .setLastExecutionUSDPrice(RandomUtils.nextDouble(0, 2000000))
                    .setPortfolioTreatmentCode("TEST-DATA")
                    .setPortfolioRelationId(RandomUtils.nextDouble(12, 2000))
                    .setEffectiveTmstmp(LocalDateTime.now().toString())
                    .setTestType("UPDATE")
                    .setHoldingsId(RandomUtils.nextLong(10000, 1000000))
                    .setRequestId(UUID.randomUUID().toString())
                    .setTradeEventCode("AMEND")
                    .setTradePrice(RandomUtils.nextDouble(0, 2000000))
                    .setOriginTradeId(UUID.randomUUID().toString())
                    .setCurrencyCode("USD")
                    .setHoldingSourceCode(RandomStringUtils.randomAlphanumeric(6))
                    .setHoldingViewCode(RandomStringUtils.randomAlphanumeric(12))
                    .setOriginalContinuousHoldingId(RandomUtils.nextLong(10000, 1000000))
                    .setPortfolioFactorVersionNumber(RandomUtils.nextLong(10000, 1000000))
                    .setPortfolioFactorMethodCode(RandomStringUtils.randomAlphanumeric(12))
                    .setTotalAmortizedBookCostUSDAmount(RandomUtils.nextDouble(0, 2000000))
                    .setHoldingEffectiveTmstmp(LocalDateTime.now().toString())
                    .setLatestPrice(false);


            // Build Message
            TotMessage totMessage = builder.build();

            // repeated field
            builderList.addTotList(totMessage.toByteString());
        }

        TotListMessage lotListMessage = builderList.build();

//        System.out.println(builderList.toString());


        // write the protocol buffers binary to a file
        try {

            byte[] compressed = Snappy.compress(lotListMessage.toByteArray());

            FileUtils.writeByteArrayToFile(new File("C:\\Users\\yhu\\Documents\\TestData\\tot-msg\\lotListMessage.bin"), lotListMessage.toByteArray());

            FileUtils.writeByteArrayToFile(new File("C:\\Users\\yhu\\Documents\\TestData\\tot-msg\\lotListMessage.bin.snappy"), compressed);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        long eTime = DateTime.now().getMillis();

        System.out.println(">>S>>"+ sTime);
        System.out.println(">>E>>"+ eTime);
        System.out.println(">>DIFF >>"+ (eTime - sTime ));

        // send as byte array
//         byte[] bytes = message.toByteArray();

        try {
//            System.out.println("Reading from file... ");
            FileInputStream fileInputStream = new FileInputStream("C:\\Users\\yhu\\Documents\\TestData\\tot-msg\\lotListMessage.bin");
            TotListMessage messageFromFile = TotListMessage.parseFrom(fileInputStream);

            List<ByteString> messageList = new ArrayList<ByteString>();
            messageList = messageFromFile.getTotListList();

//            System.out.println("Message Count >>" + messageList.size());

            for (ByteString byteString : messageList) {
                TotMessage totMessageFromFile = TotMessage.parseFrom(byteString);
//                System.out.println("setPortfolioId >>" + totMessageFromFile.getPortfolioId());
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
//        }

        }

    }
}
