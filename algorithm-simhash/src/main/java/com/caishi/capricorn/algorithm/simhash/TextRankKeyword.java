package com.caishi.capricorn.algorithm.simhash;

import org.ansj.domain.Term;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.*;

public class TextRankKeyword
{
    /**
     * 提取多少个关键字
     */
    int nKeyword = 10;
    /**
     *
     */
    final static float d = 0.85f;
    /**
     * 最大迭代次数
     */
    final static int max_iter = 100;
    final static float min_diff = 0.001f;

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

    public static Map<String, Float> getKeywordList(List<Term> parse, int size)
    {
        TextRankKeyword textRankKeyword = new TextRankKeyword();
        textRankKeyword.nKeyword = size;

        return textRankKeyword.getTermAndRank(parse, size);
    }


    public Map<String,Float> getTermAndRank(List<Term> parse)
    {
        return getRank(parse);
    }


    public Map<String,Float> getTermAndRank(List<Term> parse, Integer size)
    {
        Map<String, Float> map = getTermAndRank(parse);
        Map<String, Float> result = new LinkedHashMap<String, Float>();
        for (Map.Entry<String, Float> entry : new MaxHeap<Map.Entry<String, Float>>(size, new Comparator<Map.Entry<String, Float>>()
        {
            @Override
            public int compare(Map.Entry<String, Float> o1, Map.Entry<String, Float> o2)
            {
                return o1.getValue().compareTo(o2.getValue());
            }
        }).addAll(map.entrySet()).toList())
        {
            result.put(entry.getKey(), entry.getValue());
        }

        return result;
    }

    /**
     * 使用已经分好的词来计算rank
     * @param termList
     * @return
     */
    public Map<String,Float> getRank(List<Term> termList)
    {
        List<String> wordList = new ArrayList<String>(termList.size());
        for (Term t : termList)
        {
            if (shouldInclude(t))
            {
                wordList.add(t.getName());
            }
        }
//        System.out.println(wordList);
        Map<String, Set<String>> words = new TreeMap<String, Set<String>>();
        Queue<String> que = new LinkedList<String>();
        for (String w : wordList)
        {
            if (!words.containsKey(w))
            {
                words.put(w, new TreeSet<String>());
            }
            que.offer(w);
            if (que.size() > 10)
            {
                que.poll();
            }

            for (String w1 : que)
            {
                for (String w2 : que)
                {
                    if (w1.equals(w2))
                    {
                        continue;
                    }

                    words.get(w1).add(w2);
                    words.get(w2).add(w1);
                }
            }
        }
//        System.out.println(words);
        Map<String, Float> score = new HashMap<String, Float>();
        for (int i = 0; i < max_iter; ++i)
        {
            Map<String, Float> m = new HashMap<String, Float>();
            float max_diff = 0;
            for (Map.Entry<String, Set<String>> entry : words.entrySet())
            {
                String key = entry.getKey();
                Set<String> value = entry.getValue();
                m.put(key, 1 - d);
                for (String element : value)
                {
                    int size = words.get(element).size();
                    if (key.equals(element) || size == 0) continue;
                    m.put(key, m.get(key) + d / size * (score.get(element) == null ? 0 : score.get(element)));
                }
                max_diff = Math.max(max_diff, Math.abs(m.get(key) - (score.get(key) == null ? 0 : score.get(key))));
            }
            score = m;
            if (max_diff <= min_diff) break;
        }

        return score;
    }

    boolean shouldInclude(Term t) {
        boolean result = true;

        if(t.getName().length() < 2)
            return false;

        if (STOP_WORD.contains(t.getName())) {
            result = false;
            return result;
        }
        if (DIGIT.contains(String.valueOf(t.getName().charAt(0)))) {
            result = false;
            return result;
        }

        String pos = t.natrue().natureStr;
        Double posScore = POS_SCORE.get(pos);
        if (posScore == null) {
            result = false;
        } else if (posScore < 0.0001f) {
            result = false;
        }

        return result;
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