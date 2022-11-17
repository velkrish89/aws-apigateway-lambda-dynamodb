package com.krishna.aws;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemResult;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.*;
import java.util.HashMap;

public class OrderLambdaApi implements RequestStreamHandler {

    private String dynamoTableName = "purchase-orders";

    @Override
    public void handleRequest(InputStream input, OutputStream output, Context context) throws IOException {

        OutputStreamWriter outputWriter = new OutputStreamWriter(output);
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        JSONObject responseBody = new JSONObject();
        JSONObject responseObject = new JSONObject();
        JSONParser parser = new JSONParser();

        //DynamboDB details
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.defaultClient();
        DynamoDB dynamoDB = new DynamoDB(client);

        long orderId;
        Item responseItem = null;
        try {
            JSONObject requestObject = (JSONObject) parser.parse(reader);
            //PathParameters
            if(requestObject.get("pathParameters") != null){
                JSONObject pathParams = (JSONObject) requestObject.get("pathParameters");

                if(pathParams.get("orderId") != null) {
                    System.out.println("Order Id value from DB");
                    orderId = Long.parseLong((String) pathParams.get("orderId"));
                    responseItem = dynamoDB.getTable(dynamoTableName).getItem("orderId", orderId);
                }
            }
            //QueryStringParameters
            if(requestObject.get("queryStringParameters") != null) {
                JSONObject queryParams = (JSONObject) requestObject.get("queryStringParameters");

                if(queryParams.get("orderId") != null) {
                    orderId = Long.parseLong((String) queryParams.get("orderId"));
                    responseItem = dynamoDB.getTable(dynamoTableName).getItem("orderId", orderId);
                }
            }
            if(responseItem != null) {
                Order order = new Order(responseItem.toJSON());
                responseBody.put("order", order.toString());
                responseObject.put("statusCode", 200);
            }else {
                responseBody.put("message", "No items found with given id");
                responseObject.put("statusCode", 404);
            }
            responseObject.put("body", responseBody.toString());
            outputWriter.write(responseObject.toString());
        } catch(Exception e) {
            context.getLogger().log("ERROR : " + e.getMessage());
        } finally {
            outputWriter.close();
            reader.close();
        }
    }

    public void handlePutRequest(InputStream input, OutputStream output, Context context) throws IOException {

        OutputStreamWriter outputWriter = new OutputStreamWriter(output);
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        JSONObject responseBody = new JSONObject();
        JSONObject responseObject = new JSONObject();
        JSONParser parser = new JSONParser();

        //DynamboDB details
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.defaultClient();
        DynamoDB dynamoDB = new DynamoDB(client);
        Table table = dynamoDB.getTable(dynamoTableName);

        try {

            JSONObject requestObject = (JSONObject) parser.parse(reader);

            if(requestObject.get("body") != null) {
                System.out.println("Request Body: /n"+ (String)requestObject.get("body"));
                Order order = new Order((String)requestObject.get("body"));

                Item item = new Item()
                        .withPrimaryKey("orderId", order.getOrderId())
                        .with("productName", order.getProductName())
                        .with("quantity", order.getQuantity())
                        .with("price", order.getPrice());
                System.out.println("Item: /n" + item.toString());
                table.putItem(item);
                responseBody.put("message", "New Item created/updated");
                responseBody.put("order", order.toString());
                responseObject.put("statusCode", 200);
                responseObject.put("body", responseBody.toString());
                System.out.println("Response body: " + responseBody.toString());
            }

        } catch (Exception e) {
            responseObject.put("statusCode", 400);
            responseObject.put("ERROR", e);
            System.out.println("Error body: " + e);
            context.getLogger().log("ERROR"+ e.getMessage());
        }
            outputWriter.write(responseObject.toString());
            outputWriter.close();
            reader.close();

    }


    public void handleDeleteRequest(InputStream input, OutputStream output, Context context) throws IOException {

        OutputStreamWriter outputWriter = new OutputStreamWriter(output);
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        JSONObject responseBody = new JSONObject();
        JSONObject responseObject = new JSONObject();
        JSONParser parser = new JSONParser();

        //DynamoDB details
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.defaultClient();
        DynamoDB dynamoDB = new DynamoDB(client);
        Table table = dynamoDB.getTable(dynamoTableName);

        long orderId;
        DeleteItemResult outcome = null;
        try {
            JSONObject requestObject = (JSONObject) parser.parse(reader);

            if(requestObject.get("pathParameters") != null) {
                JSONObject pathParams = (JSONObject) requestObject.get("pathParameters");

                if(pathParams.get("orderId") != null) {
//                    orderId = Long.parseLong((String) pathParams.get("orderId"));
//                    DeleteItemSpec deleteItemSpec = new DeleteItemSpec()
//                            .withPrimaryKey("orderId", orderId);
                    DeleteItemRequest request = getDeleteItemRequest("orderId", (String) pathParams.get("orderId"));
                   outcome = client.deleteItem(request);
                }
            }
            responseBody.put("message", "Item deleted successfully!!");
            responseBody.put("result", outcome.toString());
            responseObject.put("statusCode", 200);
            responseObject.put("body", responseBody.toString());

        } catch (Exception e) {
            responseObject.put("statusCode", 400);
            responseObject.put("error", e);
            context.getLogger().log("ERROR: " + e.getMessage());
        }
            outputWriter.write(responseObject.toString());
            outputWriter.close();
            reader.close();

    }

    public DeleteItemRequest getDeleteItemRequest(String key, String value) {

        HashMap<String, AttributeValue> keyToGet = new HashMap<>();
        keyToGet.put(key, new AttributeValue().withN(value));

        DeleteItemRequest request = new DeleteItemRequest()
                .withTableName(dynamoTableName)
                .withKey(keyToGet);
        return request;
    }

}
