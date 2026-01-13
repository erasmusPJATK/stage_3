package org.ulpgc.bd.ingestion.replication;

public class ReplicationEvent {
    public int bookId;
    public String date;
    public String hour;
    public String origin;
    public String sha256Header;
    public String sha256Body;
    public String sha256Meta;
    public String parserVersion;
    public String ts;
}
