package com.example.ImpostorGame.model;

import lombok.Data;

@Data
public class ClientMessage {

    private String type;
    private String description;
    private String targetId;
    private String payLoad;
}