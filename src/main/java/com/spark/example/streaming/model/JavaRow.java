package com.spark.example.streaming.model;

import java.io.Serializable;

public class JavaRow implements Serializable {
    private String word;

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }
}
