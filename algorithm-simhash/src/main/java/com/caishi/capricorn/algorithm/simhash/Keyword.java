package com.caishi.capricorn.algorithm.simhash;

public class Keyword implements Comparable<Keyword> {
    private String name;
    private double score;
    private double idf;
    private int freq;

    public Keyword(String name, int docFreq, double weight) {
        this.name = name;
        this.idf = Math.log(10000 + 10000.0 / (docFreq + 1));
        this.score = idf * weight;
        freq++;
    }

    public Keyword(String name, double score) {
        this.name = name;
        this.score = score;
        this.idf = 1;
        freq++;
    }

    public void updateWeight(int weight) {
        this.score += weight * 1;
        freq++;
    }

    public void updateWeight(double weight) {
        this.score += weight * 1;
        freq++;
    }

    public Keyword calcScore(int nDocWords) {
        this.idf = Math.log(10000 + 10000.0 * this.freq / (nDocWords + 1));
        this.score *= this.idf;
        return this;
    }

    public int getFreq() {
        return freq;
    }

    @Override
    public int compareTo(Keyword o) {
        if (this.score < o.score) {
            return 1;
        } else {
            return -1;
        }

    }

    @Override
    public boolean equals(Object obj) {
        // TODO Auto-generated method stub
        if (obj instanceof Keyword) {
            Keyword k = (Keyword) obj;
            return k.name.equals(name);
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        // TODO Auto-generated method stub
        return name + "/" + score;// "="+score+":"+freq+":"+idf;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

}