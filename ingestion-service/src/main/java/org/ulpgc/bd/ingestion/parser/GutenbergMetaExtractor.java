package org.ulpgc.bd.ingestion.parser;

import org.ulpgc.bd.ingestion.model.Meta;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GutenbergMetaExtractor {

    private static final Pattern P_TITLE_FIELD = Pattern.compile("(?im)^\\s*Title\\s*[:\\-]\\s*(.+)$");
    private static final Pattern P_AUTHOR_FIELD = Pattern.compile("(?im)^\\s*Author\\s*[:\\-]\\s*(.+)$");
    private static final Pattern P_LANG_FIELD = Pattern.compile("(?im)^\\s*Language\\s*[:\\-]\\s*([A-Za-z][A-Za-z \\-]{0,40})$");
    private static final Pattern P_PG_LINE = Pattern.compile("(?im)^(?:the\\s+)?project\\s+gutenberg(?:'s)?\\s+e(?:\\s*|\\-)?(?:book|text)s?\\s+of\\s+(.+?)(?:\\s*,?\\s*by\\s+(.+?))?\\s*$");
    private static final Pattern P_BY_START = Pattern.compile("(?im)^\\s*(?:by|BY)\\s+([A-Z][\\p{L}.'\\- ]{1,100})\\s*$");
    private static final Pattern P_DICT_TITLE = Pattern.compile("(?im)^\\s*(?:webster'?s.*?dictionary|.*?revised\\s+unabridged\\s+dictionary.*|.*?unabridged\\s+dictionary.*)\\s*$");
    private static final Pattern P_UNDER_DIR = Pattern.compile("(?im)^\\s*Under\\s+the\\s+direction\\s+of\\s+([A-Z][\\p{L}.'\\- ]{1,100})(?:\\s*[,;].*)?$");
    private static final Pattern P_EDITED_BY = Pattern.compile("(?im)^\\s*(?:Edited|Editor)\\s*(?:by|:)\\s+([A-Z][\\p{L}.'\\- ]{1,100})(?:\\s*[,;].*)?$");
    private static final Pattern P_NAME_EDITOR_LINE = Pattern.compile("(?im)^\\s*([A-Z][\\p{L}.'\\- ]{1,100})\\s*(?:,\\s*(?:Editor|Ed\\.)|\\(\\s*(?:Editor|Ed\\.)\\s*\\))\\s*$");
    private static final Pattern P_TITLE_SUFFIX_EDITOR = Pattern.compile("(?i)^(.*?)(?:,\\s*[A-Z][\\p{L}.'\\- ]{1,80}\\s*,\\s*(?:Editor|Ed\\.))\\s*$");
    private static final Pattern P_TITLE_EDITED_BY = Pattern.compile("(?i)^(.*?)(?:,\\s*edited\\s+by\\s+.+)$");

    private static final Pattern P_YEAR_HINTED = Pattern.compile("(?is)(release\\s+date|published|publication|edition|printed|copyright|transcribed|scanned|first\\s+published|originally\\s+published).*?\\b(1[5-9]\\d{2}|20\\d{2})\\b");
    private static final Pattern P_YEAR_ANY = Pattern.compile("(?i)\\b(1[5-9]\\d{2}|20\\d{2})\\b");

    private static final Set<String> STOP_TITLE = new HashSet<>(Arrays.asList(
            "edition","release","language","produced","transcribed","illustrated","scanned","proof","copyright",
            "millennium","fulcrum","version","versione","versión","etext","ebook","project gutenberg","plain vanilla","welcome","information",
            "contents","chapter","illustration","preface","prologue","introduction","dedication","index","table of contents"
    ));
    private static final Set<String> STOP_AUTHOR_HARD = new HashSet<>(Arrays.asList(
            "the","this","that","these","those","which","whose","wherein","whereof","whereas","series","edition","copyright",
            "email","address","transcribed","produced","edited","illustrated","scanned","proof","project","gutenberg","note","preface","contents","chapter","introduction","dedication","millennium","fulcrum","version","company","co.","inc.","ltd.","press"
    ));
    private static final Set<String> PARTICLES = new HashSet<>(Arrays.asList(
            "de","di","da","del","della","van","von","der","den","la","le","du","y","af","bin","al","ibn","ap","mac","mc","fitz","st.","st"
    ));

    public Meta extract(String text, GutenbergSplitter.Markers markers) {
        String full = text == null ? "" : text.replace("\r\n", "\n").replace('\r', '\n');
        if (full.startsWith("\uFEFF")) full = full.substring(1);

        int coreStart = 0;
        int coreEnd = full.length();
        if (markers != null && markers.start != null && markers.end != null && markers.start.idx < markers.end.idx) {
            coreStart = markers.start.idx + markers.start.len;
            int lineEnd = full.indexOf('\n', markers.start.idx);
            if (lineEnd >= 0) coreStart = Math.max(coreStart, lineEnd + 1);
            coreEnd = markers.end.idx;
        }

        String pre = full.substring(0, Math.max(0, Math.min(markers != null && markers.start != null ? markers.start.idx : full.length(), 20000)));
        String coreHead = full.substring(coreStart, Math.min(coreEnd, coreStart + 20000));
        String metaRegion = pre + "\n" + coreHead;

        String title = null, author = null, language = null;
        int year = 0;

        Matcher m;
        m = P_TITLE_FIELD.matcher(metaRegion);
        if (m.find()) title = m.group(1).trim();
        m = P_AUTHOR_FIELD.matcher(metaRegion);
        if (m.find()) author = m.group(1).trim();
        m = P_LANG_FIELD.matcher(metaRegion);
        if (m.find()) language = m.group(1).trim();

        Matcher mpg = P_PG_LINE.matcher(metaRegion);
        if (mpg.find()) {
            String t = mpg.group(1) != null ? mpg.group(1).trim() : null;
            String a = mpg.group(2) != null ? mpg.group(2).trim() : null;
            if ((title == null || title.isBlank()) && t != null) title = t;
            if ((author == null || author.isBlank()) && a != null) author = a;
        }

        if (title != null) {
            Matcher me1 = P_TITLE_SUFFIX_EDITOR.matcher(title);
            if (me1.matches()) title = me1.group(1).trim();
            Matcher me2 = P_TITLE_EDITED_BY.matcher(title);
            if (me2.matches()) title = me2.group(1).trim();
            title = title.replaceAll("\\s+", " ").trim();
        }

        String[] lines = metaRegion.split("\\n", -1);

        if (title == null || title.isBlank()) {
            for (String ln : lines) {
                Matcher md = P_DICT_TITLE.matcher(ln);
                if (md.find()) {
                    title = ln.trim().replaceAll("\\s+", " ");
                    break;
                }
            }
        }

        String authorDir = null;
        Matcher mu = P_UNDER_DIR.matcher(metaRegion);
        if (mu.find()) authorDir = mu.group(1).trim().replaceFirst(",.*$", "");
        String authorEd = null;
        Matcher me = P_EDITED_BY.matcher(metaRegion);
        if (me.find()) authorEd = me.group(1).trim().replaceFirst(",.*$", "");

        List<Integer> byIdx = new ArrayList<>();
        List<String> byVals = new ArrayList<>();
        for (int i = 0; i < Math.min(lines.length, 800); i++) {
            Matcher mb = P_BY_START.matcher(lines[i]);
            if (mb.find()) {
                String cand = mb.group(1).trim();
                if (!cand.toLowerCase(Locale.ROOT).startsWith("the ")) {
                    byIdx.add(i);
                    byVals.add(cand);
                }
            }
        }

        if ((author == null || author.isBlank())) {
            if (authorDir != null && !authorDir.isBlank()) author = authorDir;
            else if (authorEd != null && !authorEd.isBlank()) author = authorEd;
        }

        if ((author == null || author.isBlank())) {
            Matcher men = P_NAME_EDITOR_LINE.matcher(metaRegion);
            String best = null;
            while (men.find()) {
                String cand = men.group(1).trim().replaceAll("\\s+", " ");
                if (best == null || cand.length() > best.length()) best = cand;
            }
            if (best != null) author = best + " (Editor)";
        }

        if ((author == null || author.isBlank()) && !byVals.isEmpty()) {
            String best = null;
            int bestScore = -1;
            for (String raw : byVals) {
                String base = raw.replaceAll("[“”]", "\"").replaceAll("\\s+", " ").trim();
                base = base.replaceFirst(",.*$", "");
                String[] toks = base.split("\\s+");
                List<String> kept = new ArrayList<>();
                for (String tk : toks) {
                    String clean = tk.replaceAll("[^\\p{L}.'\\-]", "");
                    if (clean.isEmpty()) break;
                    String low = clean.toLowerCase(Locale.ROOT);
                    if (STOP_AUTHOR_HARD.contains(low)) break;
                    boolean okToken = clean.matches("^[A-Z][\\p{L}'\\-]*$") || clean.matches("^[A-Z]\\.$") || PARTICLES.contains(low);
                    if (!okToken) break;
                    if (PARTICLES.contains(low)) kept.add(low);
                    else kept.add(clean);
                    if (kept.size() >= 6) break;
                }
                if (kept.isEmpty()) continue;
                int uc = 0;
                for (String tkn : kept) if (!PARTICLES.contains(tkn.toLowerCase(Locale.ROOT)) && (tkn.matches("^[A-Z]\\.$") || Character.isUpperCase(tkn.charAt(0)))) uc++;
                int score = uc * 10 + kept.size();
                String candidate = String.join(" ", kept);
                if (score > bestScore) { bestScore = score; best = candidate; }
            }
            if (best != null) author = best;
        }

        if (author != null) {
            String[] aa = author.split("\\s+");
            List<String> aaOut = new ArrayList<>();
            for (String tok : aa) {
                String low = tok.toLowerCase(Locale.ROOT);
                if (STOP_AUTHOR_HARD.contains(low)) break;
                if (PARTICLES.contains(low)) { aaOut.add(low); continue; }
                if (tok.matches("^[A-Z]\\.$")) { aaOut.add(tok); continue; }
                String clean = tok.replaceAll("[^\\p{L}.'\\-]", "");
                if (clean.isEmpty()) break;
                if (!Character.isUpperCase(clean.charAt(0))) break;
                aaOut.add(clean);
                if (aaOut.size() >= 6) break;
            }
            if (!aaOut.isEmpty()) author = String.join(" ", aaOut);
        }

        if (title != null) {
            String tl = title.trim();
            String low = tl.toLowerCase(Locale.ROOT);
            boolean bad = false;
            for (String s : STOP_TITLE) if (low.contains(s)) { bad = true; break; }
            if (tl.startsWith("[") || tl.startsWith("#") || low.startsWith("chapter")) bad = true;
            if (tl.matches("^[A-Z][\\p{L}']+\\.$")) bad = true;
            if (bad) title = null;
        }

        if (title == null || title.isBlank()) {
            String chosen = null;
            int bestScore = -1;
            String[] lns = lines;
            for (int idx : byIdx) {
                for (int j = idx - 1; j >= 0 && j >= idx - 8; j--) {
                    String l = lns[j].trim();
                    if (l.isEmpty()) continue;
                    String low = l.toLowerCase(Locale.ROOT);
                    boolean bad = false;
                    for (String s : STOP_TITLE) if (low.contains(s)) { bad = true; break; }
                    if (bad) continue;
                    if (l.startsWith("[") || l.startsWith("#") || low.startsWith("chapter")) continue;
                    if (l.matches("(?i)^by\\s+.+$")) continue;
                    if (l.length() < 3 || l.length() > 120) continue;
                    if (l.matches("^[A-Z][\\p{L}']+\\.$")) continue;
                    int letters = 0, upper = 0;
                    for (int k = 0; k < l.length(); k++) {
                        char c = l.charAt(k);
                        if (Character.isLetter(c)) { letters++; if (Character.isUpperCase(c)) upper++; }
                    }
                    int score = (letters == 0 ? 0 : (int)(100.0 * upper / letters)) + (8 - (idx - j));
                    if (score > bestScore) { bestScore = score; chosen = l.replaceAll("\\s+", " ").trim(); }
                }
            }
            if (chosen != null) title = chosen;
        }

        if (title == null || title.isBlank()) {
            int pickedIdx = -1;
            int best = -1;
            for (int k = 0; k < lines.length; k++) {
                String l = lines[k].trim();
                if (l.isEmpty()) continue;
                String low = l.toLowerCase(Locale.ROOT);
                boolean bad = false;
                for (String s : STOP_TITLE) if (low.contains(s)) { bad = true; break; }
                if (bad) continue;
                if (l.startsWith("[") || l.startsWith("#") || low.startsWith("chapter")) continue;
                if (l.length() < 3 || l.length() > 120) continue;
                if (l.matches("(?i)^by\\s+.+$")) continue;
                if (l.matches("^[A-Z][\\p{L}']+\\.$")) continue;
                int letters = 0, upper = 0;
                for (int i = 0; i < l.length(); i++) {
                    char c = l.charAt(i);
                    if (Character.isLetter(c)) { letters++; if (Character.isUpperCase(c)) upper++; }
                }
                int score = letters == 0 ? 0 : (int)(100.0 * upper / letters);
                boolean nearBy = false;
                for (int idx : byIdx) if (Math.abs(idx - k) <= 8) { nearBy = true; break; }
                if (nearBy) score += 15;
                if (score > best) { best = score; pickedIdx = k; }
            }
            if (pickedIdx >= 0) title = lines[pickedIdx].trim().replaceAll("\\s+", " ");
        }

        Matcher yh = P_YEAR_HINTED.matcher(metaRegion);
        if (yh.find()) {
            try {
                year = Integer.parseInt(yh.group(2));
            } catch (Exception ignored) {}
        }
        if (year == 0) {
            Matcher ya = P_YEAR_ANY.matcher(metaRegion);
            while (ya.find()) {
                int y;
                try {
                    y = Integer.parseInt(ya.group(1));
                } catch (Exception ex) {
                    continue;
                }
                int pos = ya.start();
                int start = Math.max(0, pos - 25);
                String ctx = metaRegion.substring(start, pos).toLowerCase(Locale.ROOT);
                if (ctx.contains("ebook") || ctx.contains("etext") || ctx.contains("project gutenberg") || ctx.contains("series") || ctx.contains("file") || ctx.contains("#") || ctx.contains("no.")) {
                    continue;
                }
                year = y;
                break;
            }
        }

        Meta meta = new Meta();
        meta.title = title == null || title.strip().isEmpty() ? "Unknown" : title;
        meta.author = author == null || author.strip().isEmpty() ? "Unknown" : author;
        meta.language = language != null && language.matches("^[A-Za-z][A-Za-z \\-]{0,40}$") ? language : "";
        meta.year = year;
        return meta;
    }
}
