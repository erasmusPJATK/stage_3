package org.ulpgc.bd.search.model;

public class SearchResult {
    public int book_id;
    public String title;
    public String author;
    public String language;
    public int year;
    public double score;

    public SearchResult(int book_id, String title, String author, String language, int year, double score) {
        this.book_id = book_id;
        this.title = title;
        this.author = author;
        this.language = language;
        this.year = year;
        this.score = score;
    }
}
