package org.ulpgc.bd.ingestion.parser;

import java.util.Locale;

public class GutenbergSplitter {

    private static final String[] START = new String[] {
            "*** START OF THE PROJECT GUTENBERG EBOOK",
            "*** START OF THE PROJECT GUTENBERG BOOK",
            "*** START OF THIS PROJECT GUTENBERG EBOOK",
            "*** START OF THIS PROJECT GUTENBERG",
            "START OF THE PROJECT GUTENBERG",
            "START OF THIS PROJECT GUTENBERG",
            "*** START OF PROJECT GUTENBERG"
    };
    private static final String[] END = new String[] {
            "*** END OF THE PROJECT GUTENBERG EBOOK",
            "*** END OF THE PROJECT GUTENBERG BOOK",
            "*** END OF THIS PROJECT GUTENBERG EBOOK",
            "*** END OF THIS PROJECT GUTENBERG",
            "END OF THE PROJECT GUTENBERG",
            "END OF THIS PROJECT GUTENBERG",
            "*** END OF PROJECT GUTENBERG"
    };

    public static class Match {
        public final int idx;
        public final int len;
        public Match(int idx, int len) { this.idx = idx; this.len = len; }
    }

    public static class Markers {
        public final Match start;
        public final Match end;
        public Markers(Match start, Match end) { this.start = start; this.end = end; }
    }

    public Markers findMarkers(String text) {
        if (text == null) return new Markers(null, null);
        String t = text;
        if (t.startsWith("\uFEFF")) t = t.substring(1);
        t = t.replace("\r\n", "\n").replace('\r', '\n');
        Match s = earliestOf(START, t);
        Match e = earliestOf(END, t);
        return new Markers(s, e);
    }

    public String sliceBody(String text, Markers m) {
        if (text == null) return "";
        String t = text;
        if (t.startsWith("\uFEFF")) t = t.substring(1);
        t = t.replace("\r\n", "\n").replace('\r', '\n');
        int start = 0;
        int end = t.length();
        if (m != null && m.start != null) {
            int lineEnd = t.indexOf('\n', m.start.idx);
            start = lineEnd >= 0 ? lineEnd + 1 : m.start.idx + m.start.len;
        }
        if (m != null && m.end != null) {
            int prevLine = t.lastIndexOf('\n', m.end.idx);
            end = prevLine >= 0 ? prevLine : m.end.idx;
        }
        if (start > end) { start = 0; end = t.length(); }
        return t.substring(start, end).trim();
    }

    private Match earliestOf(String[] needles, String hay) {
        int best = -1;
        int len = 0;
        String low = hay.toLowerCase(Locale.ROOT);
        for (String n : needles) {
            int i = low.indexOf(n.toLowerCase(Locale.ROOT));
            if (i >= 0 && (best == -1 || i < best)) { best = i; len = n.length(); }
        }
        return best == -1 ? null : new Match(best, len);
    }
}
