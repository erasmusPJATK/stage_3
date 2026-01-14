package es.ulpgc.bd.ingestion.replication;

public class ReplicationEvent {
    public String type;
    public String origin;
    public String target;

    public int bookId;
    public String date;
    public String hour;

    public String sha256Header;
    public String sha256Body;
    public String sha256Meta;

    public String parserVersion;
}
