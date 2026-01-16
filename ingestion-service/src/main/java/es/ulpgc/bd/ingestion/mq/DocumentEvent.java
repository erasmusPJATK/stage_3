package es.ulpgc.bd.ingestion.mq;

import com.google.gson.annotations.SerializedName;

public class DocumentEvent {

    @SerializedName("book_id")
    public int bookId;

    public String origin;

    public String[] sources;

    public DocumentEvent() {}

    public DocumentEvent(int bookId, String origin) {
        this(bookId, origin, null);
    }

    public DocumentEvent(int bookId, String origin, String[] sources) {
        this.bookId = bookId;
        this.origin = origin;
        this.sources = sources;
    }
}
