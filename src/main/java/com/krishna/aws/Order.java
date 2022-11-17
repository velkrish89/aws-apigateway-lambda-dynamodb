package com.krishna.aws;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.google.gson.Gson;

public class Order {

    private long orderId;
    private String productName;
    private int quantity;
    private double price;

    @DynamoDBHashKey
    public long getOrderId() {
        return orderId;
    }

    public void setOrderId(long orderId) {
        this.orderId = orderId;
    }

    @DynamoDBAttribute
    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    @DynamoDBAttribute
    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    @DynamoDBAttribute
    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public Order(long orderId, String productName, int quanity, double price) {
        this.orderId = orderId;
        this.productName = productName;
        this.quantity = quanity;
        this.price = price;
    }

    public Order(String orderString){
        Gson gson = new Gson();
        Order order = gson.fromJson(orderString, Order.class);
        this.orderId = order.orderId;
        this.productName = order.productName;
        this.quantity = order.quantity;
        this.price = order.price;
    }

    public String toString() {
        return new Gson().toJson(this);
    }
}
