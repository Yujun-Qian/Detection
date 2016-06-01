package com.caishi.capricorn.algorithm.simhash;

import com.caishi.capricorn.common.base.FeedConstants;
import com.caishi.capricorn.common.base.FeedMessage;

import static com.caishi.capricorn.common.base.FeedMessage.FEED_CONTENT;
import static com.caishi.capricorn.common.base.FeedMessage.FEED_PUBTIME;
import static com.caishi.capricorn.common.base.FeedMessage.FEED_SOURCE_META_PRIORITY;

import com.caishi.capricorn.common.base.FeedMessageRefer;
import com.caishi.capricorn.common.base.MessageStatus;
import com.caishi.capricorn.common.kafka.consumer.processor.JavaMsgProcessor;
import com.caishi.capricorn.common.kafka.consumer.ConsumerContainer;
import com.caishi.capricorn.common.kafka.producer.QueuedProducer;
import com.caishi.capricorn.common.kafka.consumer.processor.MsgProcessor;
import com.caishi.capricorn.common.kafka.consumer.processor.MsgProcessorInfo;
import com.caishi.capricorn.common.kafka.consumer.processor.StringMsgProcessor;

import static com.caishi.capricorn.common.base.FeedConstants.FEED_SOURCE_META_MESSAGE_STATUS;
import static com.caishi.capricorn.common.kafka.constants.KafkaConfigKey.ZK_SESSION;
import static com.caishi.capricorn.common.kafka.constants.KafkaConfigKey.ZK_SYNC;
import static com.caishi.capricorn.common.kafka.constants.KafkaConfigKey.COMMIT_TIME;

import java.math.BigInteger;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.Collections;
import java.util.Comparator;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.Date;
import java.util.Properties;
import java.util.Collection;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.Reader;
import java.io.FileNotFoundException;
import java.io.IOException;

import com.caishi.capricorn.common.utils.MD5Util;
import com.caishi.capricorn.crawler.common.crawler.CrawlRequest;
import org.ansj.domain.Term;
import org.ansj.recognition.NatureRecognition;
import org.ansj.splitWord.Analysis;
import org.ansj.splitWord.analysis.NlpAnalysis;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.ansj.util.*;
import org.ansj.recognition.*;
//import org.ansj.app.keyword.*;
//import org.ansj.app.keyword.KeyWordComputer;
//import org.ansj.app.keyword.Keyword;

import static org.ansj.util.MyStaticValue.userLibrary;
import static org.ansj.util.MyStaticValue.LIBRARYLOG;
import static org.ansj.library.UserDefineLibrary.FOREST;
import static org.ansj.library.UserDefineLibrary.ambiguityForest;
import static org.ansj.library.UserDefineLibrary.loadLibrary;

import org.ansj.util.MyStaticValue;
import org.nlpcn.commons.lang.tire.domain.Forest;
import org.nlpcn.commons.lang.tire.domain.Value;
import org.nlpcn.commons.lang.tire.domain.WoodInterface;
import org.nlpcn.commons.lang.tire.library.Library;
import org.nlpcn.commons.lang.util.IOUtil;
import org.nlpcn.commons.lang.util.StringUtil;

import com.mongodb.*;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.FindIterable;
import org.apache.commons.lang.StringUtils;
import org.bson.Document;
import com.mongodb.client.model.UpdateOptions;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONArray;

import org.junit.Assert;
import org.junit.Test;

public class SimHashTest {
    static class metaData {
        public List<String> words;
        public String id;

        metaData(List<String> words, String id) {
            this.words = words;
            this.id = id;
        }
    }

    private static Set<String> initStopWordsSet() {
        Set<String> stopWords = new HashSet<String>();

        try {
            String encoding = "UTF-8";
            File file = new File("/home/devbox-4/src/Detection/algorithm-simhash/stop_words.txt");
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

    /**
     * 加载纠正词典
     */
    private static void initAmbiguityLibrary() {
        // TODO Auto-generated method stub
        String ambiguityLibrary = MyStaticValue.ambiguityLibrary;
        if (StringUtil.isBlank(ambiguityLibrary)) {
            LIBRARYLOG.warning("init ambiguity  warning :" + ambiguityLibrary + " because : file not found or failed to read !");
            return;
        }
        ambiguityLibrary = MyStaticValue.ambiguityLibrary;
        File file = new File(ambiguityLibrary);
        if (file.isFile() && file.canRead()) {
            try {
                ambiguityForest = Library.makeForest(ambiguityLibrary);
            } catch (Exception e) {
                // TODO Auto-generated catch block
                LIBRARYLOG.warning("init ambiguity  error :" + new File(ambiguityLibrary).getAbsolutePath() + " because : not find that file or can not to read !");
                e.printStackTrace();
            }
            LIBRARYLOG.info("init ambiguityLibrary ok!");
        } else {
            LIBRARYLOG.warning("init ambiguity  warning :" + new File(ambiguityLibrary).getAbsolutePath() + " because : file not found or failed to read !");
        }
    }

    /**
     * 加载用户自定义词典和补充词典
     */
    private static void initUserLibrary() {
        // TODO Auto-generated method stub
        try {
            FOREST = new Forest();
            // 加载用户自定义词典
            String userLibrary = MyStaticValue.userLibrary;
            loadLibrary(FOREST, userLibrary);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    static metaData process(String title, String content, Set<String> stopWords) {
        String DEBUG = "false";
        /*
        try {
            InputStream is = NewsSimHash.class.getClassLoader().getResourceAsStream("config.properties");
            if (is != null) {
                Properties prop = new Properties();
                try {
                    prop.load(is);
                    for (Entry<Object, Object> entry : prop.entrySet()) {
                        String key = (String) entry.getKey();
                        String value = (String) entry.getValue();
                        System.out.println(key);
                        System.out.println(value);
                    }
                    DEBUG = prop.getProperty("DEBUG");
                } catch (IOException e) {
                    e.printStackTrace();
                    for (StackTraceElement elem : e.getStackTrace()) {
                        System.out.println(elem);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        */
        System.out.println("**************** Thread ID is: " + Thread.currentThread().getId());

        int sourceId = 0;
        String crawlId = null;

        int contentLength = content.length();
        int titleLength = 0;
        if (title != null) {
            titleLength = title.length();
        }

        if (title != null) {
            System.out.println("****************" + title);
        }

        if (title != null) {
            content = title + " " + content;
        }

        content = NewsSimHash.processContent(content);

        List<String> sentences = NewsSimHash.getLongestSentences(content);

        System.out.println("content is: " + content);

        final NewsSimHash ns = new NewsSimHash(stopWords, content, true);
        Map<String, Integer> wordsMap = ns.getWordsMap();
        List<Term> parse = ns.getParse();
        String id = ns.getIntSimHash().toString(16);


        System.out.println("****************" + id);

        String combinedSentence = null;
        for (String sentence : sentences) {
            combinedSentence += sentence;
        }
        System.out.println("****************" + combinedSentence);

        return new metaData(ns.getWords(), id);
    }


    @Test
    public void testSimhash() {
        final Set<String> stopWords = initStopWordsSet();
        Set<String> sensitiveWords = new HashSet<String>();

        System.out.println(org.ansj.util.MyStaticValue.userLibrary);
        org.ansj.util.MyStaticValue.userLibrary = "/home/hadoop/software/default.dic";
        org.ansj.util.MyStaticValue.ambiguityLibrary = "/home/hadoop/software/ambiguity.dic";

        initUserLibrary();
        initAmbiguityLibrary();
        
        String Queue_IPAddress1 = null;
        String Queue_IPAddress2 = null;
        String Queue_IPAddress3 = null;
        String Kafka_IPAddress1 = null;
        String Kafka_IPAddress2 = null;
        String Kafka_IPAddress3 = null;
        String SimhashDB_IPAddress = null;
        String NewsDB_IPAddress = null;
        String DEBUG = "false";
        int NewsDB_Port = 0;

        try {
            InputStream is = NewsSimHash.class.getClassLoader().getResourceAsStream("config.properties");
            if (is != null) {
                Properties prop = new Properties();
                prop.load(is);
                SimhashDB_IPAddress = "";
                NewsDB_IPAddress = "10.3.1.9";
                NewsDB_Port = 30000;
                System.out.println("test stage");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }


        metaData result1 = null;
        metaData result2 = null;

        MongoClient mongoClient = new MongoClient(Arrays.asList(new ServerAddress(NewsDB_IPAddress, NewsDB_Port)),
                Arrays.asList(MongoCredential.createCredential("news", "news", "news9icaishi".toCharArray())));
        final MongoDatabase newsDB = mongoClient.getDatabase("news");
        final MongoCollection newsContent = newsDB.getCollection("newsContent");


        /*
        MongoClient mongoClient1 = new MongoClient(Arrays.asList(new ServerAddress(NewsDB_IPAddress, NewsDB_Port)));
        final MongoDatabase db1 = mongoClient1.getDatabase("news");
        MongoCollection newsContent = db1.getCollection("newsContent");
        */

        //String newsId1 = "200006bb5265763c";
        //String newsId1 = "20000251c76d7554";
          String newsId1 = "208072e81a3001de";
        //String newsId1 = "ee7f2314d041ad14";
        FindIterable iterable = newsContent.find(new Document("_id", newsId1));
        MongoCursor cursor = iterable.iterator();
        while (cursor.hasNext()) {
            Document document = (Document)cursor.next();

            //System.out.println(document);
            //System.out.println(document.toJson());

            String newsStr = document.toJson();
            //System.out.println("**************** newsStr is: " + newsStr);
            JSONObject newsObj = (JSONObject)JSON.parse(newsStr);
            String title = (String)newsObj.get("title");
            String content = (String)newsObj.get("content");
            result1 = process(title, content, stopWords);
        }
        cursor.close();

        //String newsId2 = "2bb4947f764";
        //String newsId2 = "20000251676dc556";
        String newsId2 = "24c17ae89a1221de";
        //String newsId2 = "90c4b8cba6327ca2";
        //String newsId2 = "2000079abe3fd6b7";
        iterable = newsContent.find(new Document("_id", newsId2));
        cursor = iterable.iterator();
        while (cursor.hasNext()) {
            Document document = (Document)cursor.next();

            //System.out.println(document);
             //System.out.println(document.toJson());

            String newsStr = document.toJson();
            //System.out.println("**************** newsStr is: " + newsStr);
            JSONObject newsObj = (JSONObject)JSON.parse(newsStr);
            String title = (String)newsObj.get("title");
            String content = (String)newsObj.get("content");
            result2 = process(title, content, stopWords);
        }
        cursor.close();

        System.out.println("the hamming distance is : " + NewsSimHash.hammingDistance(newsId1, newsId2));
        System.out.println("the new hamming distance is : " + NewsSimHash.hammingDistance(result1.id, result2.id));

        System.out.println("the hamming distance is : " + NewsSimHash.hammingDistance(newsId1, result1.id));
        System.out.println("the new hamming distance is : " + NewsSimHash.hammingDistance(newsId2, result2.id));

        System.out.println(org.ansj.util.MyStaticValue.userLibrary);

        List<String> words1 = result1.words;
        List<String> words2 = result2.words;

        System.out.println("words only present in news one are: ");
        for (String word : words1) {
            if (!words2.contains(word)) {
                System.out.println(word);
            }
        }

        System.out.println("words only present in news two are: ");
        for (String word : words2) {
            if (!words1.contains(word)) {
                System.out.println(word);
            }
        }
    }

    /*
    @Test
    public void testSimhash1() {
        final Set<String> stopWords = initStopWordsSet();
        Set<String> sensitiveWords = new HashSet<String>();

        String Queue_IPAddress1 = null;
        String Queue_IPAddress2 = null;
        String Queue_IPAddress3 = null;
        String Kafka_IPAddress1 = null;
        String Kafka_IPAddress2 = null;
        String Kafka_IPAddress3 = null;
        String SimhashDB_IPAddress = null;
        String NewsDB_IPAddress = null;
        String DEBUG = "false";
        int NewsDB_Port = 0;

        try {
            InputStream is = NewsSimHash.class.getClassLoader().getResourceAsStream("config.properties");
            if (is != null) {
                Properties prop = new Properties();
                prop.load(is);
                SimhashDB_IPAddress = "";
                NewsDB_IPAddress = "10.1.1.122";
                NewsDB_Port = 27018;
                System.out.println("test production");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        metaData result1 = null;
        metaData result2 = null;

        MongoClient mongoClient1 = new MongoClient(Arrays.asList(new ServerAddress(NewsDB_IPAddress, NewsDB_Port)));
        final MongoDatabase db1 = mongoClient1.getDatabase("news");
        MongoCollection newsContent = db1.getCollection("newsContent");

        String newsId1 = "85d04099f213020b";
        //String newsId1 = "20000251c76d7554";
        FindIterable iterable = newsContent.find(new Document("_id", newsId1));
        MongoCursor cursor = iterable.iterator();
        while (cursor.hasNext()) {
            Document document = (Document)cursor.next();

            //System.out.println(document);
            //System.out.println(document.toJson());

            String newsStr = document.toJson();
            //System.out.println("**************** newsStr is: " + newsStr);
            JSONObject newsObj = (JSONObject)JSON.parse(newsStr);
            String title = (String)newsObj.get("title");
            String content = (String)newsObj.get("content");
            result1 = process(title, content, stopWords);
        }
        cursor.close();

        String newsId2 = "add06099f2520219";
        //String newsId2 = "20000251676dc556";
        iterable = newsContent.find(new Document("_id", newsId2));
        cursor = iterable.iterator();
        while (cursor.hasNext()) {
            Document document = (Document)cursor.next();

            //System.out.println(document);
            //System.out.println(document.toJson());

            String newsStr = document.toJson();
            //System.out.println("**************** newsStr is: " + newsStr);
            JSONObject newsObj = (JSONObject)JSON.parse(newsStr);
            String title = (String)newsObj.get("title");
            String content = (String)newsObj.get("content");
            result2 = process(title, content, stopWords);
        }
        cursor.close();

        System.out.println("the hamming distance is : " + NewsSimHash.hammingDistance(newsId1, newsId2));
        System.out.println("the new hamming distance is : " + NewsSimHash.hammingDistance(result1.id, result2.id));

        List<String> words1 = result1.words;
        List<String> words2 = result2.words;

        System.out.println("words only present in news one are: ");
        for (String word : words1) {
            if (!words2.contains(word)) {
                System.out.println(word);
            }
        }

        System.out.println("words only present in news two are: ");
        for (String word : words2) {
            if (!words1.contains(word)) {
                System.out.println(word);
            }
        }
    }
    */

    @Test
    public void testTag() {
        final Set<String> stopWords = initStopWordsSet();
        Set<String> sensitiveWords = new HashSet<String>();

        String Queue_IPAddress1 = null;
        String Queue_IPAddress2 = null;
        String Queue_IPAddress3 = null;
        String Kafka_IPAddress1 = null;
        String Kafka_IPAddress2 = null;
        String Kafka_IPAddress3 = null;
        String SimhashDB_IPAddress = null;
        String NewsDB_IPAddress = null;
        String DEBUG = "false";
        int NewsDB_Port = 0;

        try {
            InputStream is = NewsSimHash.class.getClassLoader().getResourceAsStream("config.properties");
            if (is != null) {
                Properties prop = new Properties();
                prop.load(is);
                SimhashDB_IPAddress = "";
                NewsDB_IPAddress = "10.3.1.9";
                NewsDB_Port = 30000;
                System.out.println("test production");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        metaData result1 = null;
        metaData result2 = null;

        MongoClient mongoClient = new MongoClient(Arrays.asList(new ServerAddress(NewsDB_IPAddress, NewsDB_Port)),
                Arrays.asList(MongoCredential.createCredential("news", "news", "news9icaishi".toCharArray())));
        final MongoDatabase newsDB = mongoClient.getDatabase("news");
        final MongoCollection newsContent = newsDB.getCollection("newsContent");

        /*
        MongoClient mongoClient1 = new MongoClient(Arrays.asList(new ServerAddress(NewsDB_IPAddress, NewsDB_Port)));
        final MongoDatabase db1 = mongoClient1.getDatabase("news");
        MongoCollection newsContent = db1.getCollection("newsContent");
        */

        String newsId1 = "6ecad2032e506705";
        //String newsId1 = "e6dbe2822a4c5432";
        FindIterable iterable = newsContent.find(new Document("_id", newsId1));
        MongoCursor cursor = iterable.iterator();
        while (cursor.hasNext()) {
            Document document = (Document)cursor.next();

            //System.out.println(document);
            //System.out.println(document.toJson());

            String newsStr = document.toJson();
            //System.out.println("**************** newsStr is: " + newsStr);
            JSONObject newsObj = (JSONObject)JSON.parse(newsStr);
            String title = (String)newsObj.get("title");
            String original_content = (String)newsObj.get("content");
            String content = NewsSimHash.processContent(title + original_content);

            final NewsSimHash ns = new NewsSimHash(stopWords, content, false);
            Map<String, Integer> wordsMap = ns.getWordsMap();
            List<Term> parse = ns.getParse();

            KeyWordComputer kwc = new KeyWordComputer(20);
            Collection<Keyword> result = kwc.computeArticleTfidf(parse, content.length(), title.length());
            {
                System.out.println(title);
                System.out.println(content);
                System.out.println(result);
            }
        }
        cursor.close();

        newsId1 = "";
        //String newsId1 = "e6dbe2822a4c5432";
        iterable = newsContent.find(new Document("_id", newsId1));
        cursor = iterable.iterator();
        while (cursor.hasNext()) {
            Document document = (Document)cursor.next();

            //System.out.println(document);
            //System.out.println(document.toJson());

            String newsStr = document.toJson();
            //System.out.println("**************** newsStr is: " + newsStr);
            JSONObject newsObj = (JSONObject)JSON.parse(newsStr);
            String title = (String)newsObj.get("title");
            String original_content = (String)newsObj.get("content");
            String content = NewsSimHash.processContent(title + original_content);

            final NewsSimHash ns = new NewsSimHash(stopWords, content, false);
            Map<String, Integer> wordsMap = ns.getWordsMap();
            List<Term> parse = ns.getParse();

            KeyWordComputer kwc = new KeyWordComputer(20);
            Collection<Keyword> result = kwc.computeArticleTfidf(parse, content.length(), title.length());
            {
                System.out.println(title);
                System.out.println(content);
                System.out.println(result);
            }
        }
        cursor.close();
    }
}
