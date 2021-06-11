package org.apache.flink.util;

import java.sql.Timestamp;

public class Order {
    public Long user;
    public String product;
    public int amount;
    public Timestamp ts;

    public Order() {
    }

    public Order(Long user, String product, int amount) {
        this.user = user;
        this.product = product;
        this.amount = amount;
    }

    public Order(Long user, String product, int amount, Timestamp time) {
        this.user = user;
        this.product = product;
        this.amount = amount;
        this.ts = time;
    }

    @Override
    public String toString() {
        return "Order{" +
                "user=" + user +
                ", product='" + product + '\'' +
                ", amount=" + amount +
                ", ts=" + ts +
                '}';
    }
}