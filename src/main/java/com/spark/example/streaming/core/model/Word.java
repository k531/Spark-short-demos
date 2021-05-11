package com.spark.example.streaming.core.model;

import java.io.Serializable;

public class Word implements Serializable {
    private String word;

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }
}
