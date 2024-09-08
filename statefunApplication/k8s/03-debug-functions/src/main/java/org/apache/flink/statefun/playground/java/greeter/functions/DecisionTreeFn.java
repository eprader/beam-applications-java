package org.apache.flink.statefun.playground.java.greeter.functions;

import org.apache.flink.statefun.playground.java.greeter.tasks.AbstractTask;
import org.apache.flink.statefun.playground.java.greeter.tasks.DecisionTreeClassify;
import org.apache.flink.statefun.playground.java.greeter.types.BlobReadEntry;
import org.apache.flink.statefun.playground.java.greeter.types.DecisionTreeEntry;
import org.apache.flink.statefun.playground.java.greeter.types.SenMlEntry;
import org.apache.flink.statefun.sdk.java.Context;
import org.apache.flink.statefun.sdk.java.StatefulFunction;
import org.apache.flink.statefun.sdk.java.StatefulFunctionSpec;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.message.MessageBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import weka.classifiers.trees.J48;
import weka.core.SerializationHelper;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.statefun.playground.java.greeter.types.Types.*;

public class DecisionTreeFn implements StatefulFunction {

    static final TypeName TYPENAME = TypeName.typeNameFromString("pred/decisionTree");
    public static final StatefulFunctionSpec SPEC =
            StatefulFunctionSpec.builder(TYPENAME)
                    .withSupplier(DecisionTreeFn::new)
                    .build();
    static final TypeName INBOX = TypeName.typeNameFromString("pred/mqttPublish");


    public  int calculatePrimes(int limit) {
        int count = 0;
        for (int i = 2; i <= limit; i++) {
            if (isPrime(i)) {
                count++;
            }
        }
        return count;
    }

    public  boolean isPrime(int number) {
        if (number <= 1) {
            return false;
        }
        for (int i = 2; i * i <= number; i++) {
            if (number % i == 0) {
                return false;
            }
        }
        return true;
    }


    @Override
    public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {

        try {
            long arrivalTime;
            if (message.is(SENML_ENTRY_JSON_TYPE)) {

                SenMlEntry senMlEntry = message.as(SENML_ENTRY_JSON_TYPE);
                String sensorMeta = senMlEntry.getMeta();

                String obsVal = "0";
                String msgId = "0";
                HashMap<String, String> map = new HashMap<>();
                map.put(AbstractTask.DEFAULT_KEY, obsVal);
                calculatePrimes(1000);
                Float res = 0f;
                if (res != null) {
                    if (res != Float.MIN_VALUE) {
                        DecisionTreeEntry decisionTreeEntry = new DecisionTreeEntry(sensorMeta, obsVal, msgId, res.toString(), "DTC", senMlEntry.getDataSetType());
                        //decisionTreeEntry.setArrivalTime(senMlEntry.getArrivalTime());
                        context.send(
                                MessageBuilder.forAddress(INBOX, String.valueOf(decisionTreeEntry.getMsgid()))
                                        .withCustomType(DECISION_TREE_ENTRY_JSON_TYPE, decisionTreeEntry)
                                        .build());
                    } else {
                        throw new RuntimeException();
                    }
                }


            } else if (message.is(BLOB_READ_ENTRY_JSON_TYPE)) {
                BlobReadEntry blobReadEntry = message.as(BLOB_READ_ENTRY_JSON_TYPE);
                arrivalTime = blobReadEntry.getArrivalTime();
                String msgtype = blobReadEntry.getMsgType();
                String analyticsType = blobReadEntry.getAnalyticType();
                String sensorMeta = blobReadEntry.getMeta();

                String obsVal = "0";
                String msgId = "0";
                calculatePrimes(1000);
                Float res = 12f;
                if (res != null) {
                    if (res != Float.MIN_VALUE) {
                        DecisionTreeEntry decisionTreeEntry = new DecisionTreeEntry(sensorMeta, obsVal, msgId, res.toString(), "DTC", blobReadEntry.getDataSetType());
                        //decisionTreeEntry.setArrivalTime(arrivalTime);
                        context.send(
                                MessageBuilder.forAddress(INBOX, String.valueOf(decisionTreeEntry.getMsgid()))
                                        .withCustomType(DECISION_TREE_ENTRY_JSON_TYPE, decisionTreeEntry)
                                        .build());
                    } else {
                        throw new RuntimeException("Error when classifying");
                    }
                }

            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return context.done();

    }
}