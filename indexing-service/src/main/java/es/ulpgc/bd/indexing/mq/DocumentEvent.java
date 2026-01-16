package es.ulpgc.bd.indexing.mq;

import com.google.gson.annotations.SerializedName;

public class DocumentEvent {

    @SerializedName(value = "book_id", alternate = {"bookId"})
    public int bookId;

    public String origin;

    public DocumentEvent() {}

    public DocumentEvent(int bookId, String origin) {
        this.bookId = bookId;
        this.origin = origin;
    }
}
