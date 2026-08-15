package com.example.ImpostorGame.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class WordPair {

    private String normalWord;
    private String imposterWord;
}