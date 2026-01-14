package es.ulpgc.bd.ingestion.replication;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DatalakeScanner {
    private final Path datalake;

    private static final Pattern P = Pattern.compile("^(\\d+)_(header|body|meta)\\.(txt|json)$");

    public DatalakeScanner(Path datalake) {
        this.datalake = datalake;
    }

    public Map<Integer, Path[]> latestHeaderBodyMetaByBook() {
        Map<Integer, Best> best = new HashMap<>();
        if (datalake == null || !Files.exists(datalake)) return new HashMap<>();

        try (var stream = Files.walk(datalake)) {
            for (Path p : (Iterable<Path>) stream::iterator) {
                if (p == null || !Files.isRegularFile(p)) continue;
                String fn = p.getFileName() != null ? p.getFileName().toString() : "";
                Matcher m = P.matcher(fn);
                if (!m.matches()) continue;

                int id = Integer.parseInt(m.group(1));
                String kind = m.group(2);

                String[] dh = dateHourOf(p);
                String date = dh[0];
                String hour = dh[1];

                Best cur = best.get(id);
                if (cur == null || isAfter(date, hour, cur.date, cur.hour)) {
                    cur = new Best(date, hour);
                    best.put(id, cur);
                } else if (!date.equals(cur.date) || !hour.equals(cur.hour)) {
                    continue;
                }

                if (kind.equals("header")) cur.header = p;
                else if (kind.equals("body")) cur.body = p;
                else cur.meta = p;
            }
        } catch (IOException ignored) {}

        Map<Integer, Path[]> out = new HashMap<>();
        for (Map.Entry<Integer, Best> e : best.entrySet()) {
            Best b = e.getValue();
            if (b.header != null && b.body != null) out.put(e.getKey(), new Path[]{b.header, b.body, b.meta});
        }
        return out;
    }

    public static String[] dateHourOf(Path file) {
        try {
            Path hour = file.getParent();
            Path date = hour != null ? hour.getParent() : null;
            String d = date != null && date.getFileName() != null ? date.getFileName().toString() : "00000000";
            String h = hour != null && hour.getFileName() != null ? hour.getFileName().toString() : "00";
            return new String[]{d, h};
        } catch (Exception e) {
            return new String[]{"00000000", "00"};
        }
    }

    private static boolean isAfter(String d1, String h1, String d2, String h2) {
        int cd = d1.compareTo(d2);
        if (cd != 0) return cd > 0;
        return h1.compareTo(h2) > 0;
    }

    private static class Best {
        final String date;
        final String hour;
        Path header;
        Path body;
        Path meta;
        Best(String date, String hour) {
            this.date = date;
            this.hour = hour;
        }
    }
}
