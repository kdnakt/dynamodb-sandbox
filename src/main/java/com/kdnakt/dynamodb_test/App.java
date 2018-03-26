package com.kdnakt.dynamodb_test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TimeToLiveSpecification;
import com.amazonaws.services.dynamodbv2.model.UpdateTimeToLiveRequest;

public class App {

    private static final String TABLE_NAME = "test-table-1";
    private static final String ATTR_ID = "id";
    private static final String ATTR_VAL = "value";
    private static final String ATTR_TTL = "ttl";

    public static void main(String[] args) {
        final AmazonDynamoDB dynamo = AmazonDynamoDBClientBuilder.standard()
                .withCredentials(new SystemPropertiesCredentialsProvider()).withEndpointConfiguration(
                        new EndpointConfiguration("http://localhost:8008", Regions.DEFAULT_REGION.name()))
                .build();

        final CreateTableRequest createTable = new CreateTableRequest().withTableName(TABLE_NAME)
                .withKeySchema(new KeySchemaElement(ATTR_ID, KeyType.HASH))
                .withAttributeDefinitions(new AttributeDefinition(ATTR_ID, ScalarAttributeType.S))
                .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L));
        try {
            dynamo.createTable(createTable);
            sleep();
        } catch (ResourceInUseException ignore) {
        }

        final TimeToLiveSpecification ttlSpec = new TimeToLiveSpecification().withAttributeName(ATTR_TTL)
                .withEnabled(true);
        final UpdateTimeToLiveRequest updateTtl = new UpdateTimeToLiveRequest().withTableName(TABLE_NAME)
                .withTimeToLiveSpecification(ttlSpec);
        try {
            dynamo.updateTimeToLive(updateTtl);
            sleep();
        } catch (AmazonDynamoDBException ignore) {
        }

        Map<String, AttributeValue> item = new HashMap<>();
        item.put(ATTR_ID, new AttributeValue().withS("id-1"));
        item.put(ATTR_VAL, new AttributeValue().withS("val-1"));
        long ttl = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(1);
        System.out.println(ttl);
        item.put(ATTR_TTL, new AttributeValue().withN(String.valueOf(ttl)));
        // dynamo.putItem(TABLE_NAME, item);
        sleep();

        item = new HashMap<>();
        item.put(ATTR_ID, new AttributeValue().withS("id-1"));
        GetItemResult res = dynamo.getItem(TABLE_NAME, item);
        if (res != null) {
            res.getItem().values().stream().forEach(v -> System.out.println(v.getS() != null ? v.getS() : v.getN()));
            System.out.println(System.currentTimeMillis());
        } else {
            System.out.println("no data");
        }
        sleep();
        sleep();
        sleep();

        res = dynamo.getItem(TABLE_NAME, item);
        if (res != null) {
            res.getItem().values().stream().forEach(v -> System.out.println(v.getS() != null ? v.getS() : v.getN()));
            System.out.println(System.currentTimeMillis());
        } else {
            System.out.println("no data");
        }
        sleep();
        sleep();
        sleep();
        res = dynamo.getItem(TABLE_NAME, item);
        if (res != null) {
            res.getItem().values().stream().forEach(v -> System.out.println(v.getS() != null ? v.getS() : v.getN()));
            System.out.println(System.currentTimeMillis());
        } else {
            System.out.println("no data");
        }
    }

    private static void sleep() {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
