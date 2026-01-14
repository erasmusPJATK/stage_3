package es.ulpgc.bd.indexing.util;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DatalakeScanner {

    private final Path datalake;

    public DatalakeScanner(Path datalake) {
        this.datalake = datalake;
    }

    public Path[] findLatestHeaderBodyMeta(int bookId) throws IOException {
        if (!Files.exists(datalake)) return null;

        Pattern pH = Pattern.compile("^" + bookId + "_header\\.txt$");
        Pattern pB = Pattern.compile("^" + bookId + "_body\\.txt$");
        Pattern pM = Pattern.compile("^" + bookId + "_meta\\.json$");

        Path bestH = null, bestB = null, bestM = null;
        String bestKey = null;

        try (var s = Files.walk(datalake)) {
            for (Path p : (Iterable<Path>) s::iterator) {
                String fn = p.getFileName() != null ? p.getFileName().toString() : "";
                Matcher mh = pH.matcher(fn);
                Matcher mb = pB.matcher(fn);
                Matcher mm = pM.matcher(fn);

                if (!mh.matches() && !mb.matches() && !mm.matches()) continue;

                String key = dateHourKey(p);
                if (key == null) continue;

                if (bestKey == null || key.compareTo(bestKey) > 0) {
                    bestKey = key;
                    bestH = null; bestB = null; bestM = null;
                }
                if (key.equals(bestKey)) {
                    if (mh.matches()) bestH = p;
                    if (mb.matches()) bestB = p;
                    if (mm.matches()) bestM = p;
                }
            }
        }

        if (bestH == null || bestB == null) return null;
        return new Path[]{bestH, bestB, bestM};
    }

    private String dateHourKey(Path p) {
        Path hour = p.getParent();
        if (hour == null) return null;
        Path date = hour.getParent();
        if (date == null) return null;

        String d = date.getFileName() != null ? date.getFileName().toString() : null;
        String h = hour.getFileName() != null ? hour.getFileName().toString() : null;
        if (d == null || h == null) return null;
        if (d.length() != 8) return null;
        if (h.length() == 1) h = "0" + h;
        return d + "_" + h;
    }
}
