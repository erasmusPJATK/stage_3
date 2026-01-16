package es.ulpgc.bd.ingestion.mq;

import com.google.gson.annotations.SerializedName;

public class DocumentEvent {

    @SerializedName("book_id")
    public int bookId;

    public String origin;

    public DocumentEvent() {}

    public DocumentEvent(int bookId, String origin) {
        this.bookId = bookId;
        this.origin = origin;
    }
}
