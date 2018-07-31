package com.comarch.kafkacomsumer.model;

import lombok.Data;

@Data
public class Car {
    private String brand;
    private String model;
    private int price;
    private int mileage;
    private String owner;
}
