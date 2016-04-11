package com.caishi.capricorn.algorithm.simhash;

import java.security.Key;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.Set;
import java.util.HashSet;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;

import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.NlpAnalysis;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.nlpcn.commons.lang.util.StringUtil;

public class KeyWordComputer {

    private static final Map<String, Double> POS_SCORE = new HashMap<String, Double>();
    private static final Set<String> DIGIT = new TreeSet<String>();
    private static Set<String> STOP_WORD = new HashSet<String>();


    static {
        POS_SCORE.put("null", 0.0);
        POS_SCORE.put("w", 0.0);
        POS_SCORE.put("i", 0.0);
        POS_SCORE.put("en", 0.0);
        POS_SCORE.put("num", 0.0);
        POS_SCORE.put("nr", 2.0);
        POS_SCORE.put("nrf", 2.0);
        POS_SCORE.put("nw", 2.0);
        POS_SCORE.put("nt", 2.0);
        POS_SCORE.put("l", 0.2);
        POS_SCORE.put("a", 0.2);
        POS_SCORE.put("nz", 0.0);
        POS_SCORE.put("v", 0.5);

        DIGIT.add("0");
        DIGIT.add("1");
        DIGIT.add("2");
        DIGIT.add("3");
        DIGIT.add("4");
        DIGIT.add("5");
        DIGIT.add("6");
        DIGIT.add("7");
        DIGIT.add("8");
        DIGIT.add("9");
        DIGIT.add("零");
        DIGIT.add("一");
        DIGIT.add("二");
        DIGIT.add("两");
        DIGIT.add("三");
        DIGIT.add("四");
        DIGIT.add("五");
        DIGIT.add("六");
        DIGIT.add("七");
        DIGIT.add("八");
        DIGIT.add("九");
        DIGIT.add("这");
        DIGIT.add("那");
        DIGIT.add("几");
        DIGIT.add("第");
        DIGIT.add("背");

        STOP_WORD = initStopWordsSet();
    }

    private int nKeyword = 5;

    public KeyWordComputer() {
    }

    /**
     * 返回关键词个数
     *
     * @param nKeyword
     */
    public KeyWordComputer(int nKeyword) {
        this.nKeyword = nKeyword;

    }

    /**
     * @param content 正文
     * @return
     */
    private List<Keyword> computeArticleTfidf(String content, int titleLength) {
        Map<String, Keyword> tm = new HashMap<String, Keyword>();

        int cWord = 0;
        List<Term> parse = ToAnalysis.parse(content);
        for (Term term : parse) {
            if (STOP_WORD.contains(term.getName())) {
                continue;
            }
            if (DIGIT.contains(String.valueOf(term.getName().charAt(0)))) {
                continue;
            }

            double weight = getWeight(term, content.length(), titleLength);
            if (weight == 0)
                continue;
            cWord++;
            Keyword keyword = tm.get(term.getName());
            if (keyword == null) {
                keyword = new Keyword(term.getName(), weight);
                tm.put(term.getName(), keyword);
            } else {
                keyword.updateWeight(weight);
            }
        }

        TreeSet<Keyword> treeSet = new TreeSet<Keyword>();

        for (Keyword keyword : tm.values()) {
            treeSet.add(keyword.calcScore(cWord));
        }

        ArrayList<Keyword> arrayList = new ArrayList<Keyword>(treeSet);
        if (treeSet.size() <= nKeyword) {
            return arrayList;
        } else {
            return arrayList.subList(0, nKeyword);
        }

    }

    /**
     * @param title   标题
     * @param content 正文
     * @return
     */
    public List<Keyword> computeArticleTfidf(String title, String content) {
        if (StringUtil.isBlank(title)) {
            title = "";
        }
        if (StringUtil.isBlank(content)) {
            content = "";
        }
        return computeArticleTfidf(title + "\t" + content, title.length());
    }

    /**
     * @param content 正文
     * @return
     */
    public List<Keyword> computeArticleTfidf(List<Term> parse, int contentLength, int titleLength) {
        Map<String, Keyword> tm = new HashMap<String, Keyword>();

        int cWord = 0;
        for (Term term : parse) {
            if (STOP_WORD.contains(term.getName())) {
                continue;
            }
            if (DIGIT.contains(String.valueOf(term.getName().charAt(0)))) {
                continue;
            }

            double weight = getWeight(term, contentLength, titleLength);
            if (weight == 0)
                continue;
            cWord++;
            Keyword keyword = tm.get(term.getName());
            if (keyword == null) {
                keyword = new Keyword(term.getName(), weight);
                tm.put(term.getName(), keyword);
            } else {
                keyword.updateWeight(weight);
            }
        }

        TreeSet<Keyword> treeSet = new TreeSet<Keyword>();

        for (Keyword keyword : tm.values()) {
            treeSet.add(keyword.calcScore(cWord));
        }

        ArrayList<Keyword> arrayList = new ArrayList<Keyword>(treeSet);
        if (treeSet.size() <= nKeyword) {
            return arrayList;
        } else {
            return arrayList.subList(0, nKeyword);
        }

    }


    /**
     * 只有正文
     *
     * @param content
     * @return
     */
    public List<Keyword> computeArticleTfidf(String content) {
        return computeArticleTfidf(content, 0);
    }

    private double getWeight(Term term, int length, int titleLength) {
        if (term.getName().trim().length() < 2) {
            return 0;
        }

        String pos = term.natrue().natureStr;

        Double posScore = POS_SCORE.get(pos);

                /*
        System.out.println(term.getName());
		System.out.println(term.getNatureStr());
		System.out.println(posScore);
		System.out.println(term.getName().charAt(0));
                */

        if (posScore == null) {
            posScore = 1.0;
        } else if (posScore == 0) {
            return 0;
        }

        if (titleLength > term.getOffe()) {
            return 5 * posScore;
        }
        return (length - term.getOffe()) * posScore / (double) length;
    }

    private static Set<String> initStopWordsSet() {
        Set<String> stopWords = new HashSet<String>();

        try {
            String encoding = "UTF-8";
            File file = new File("/home/hadoop/software/stop_words1.txt");
            if (file.isFile() && file.exists()) {
                InputStreamReader read = new InputStreamReader(
                        new FileInputStream(file), encoding);
                BufferedReader bufferedReader = new BufferedReader(read);
                String line = null;
                while ((line = bufferedReader.readLine()) != null) {
                    stopWords.add(line);
                }
                read.close();
            } else {
                System.out.println("File Not Found");
            }
        } catch (Exception e) {
            System.out.println("error reading stop words");
            e.printStackTrace();
        }

        return stopWords;
    }
}
