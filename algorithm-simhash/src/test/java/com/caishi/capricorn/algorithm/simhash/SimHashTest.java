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
import java.util.Date;

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

import java.util.List;
import java.util.Properties;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.*;
import edu.stanford.nlp.time.*;
import edu.stanford.nlp.util.CoreMap;

import de.unihd.dbs.heideltime.standalone.HeidelTimeStandalone;
import de.unihd.dbs.heideltime.standalone.DocumentType;
import de.unihd.dbs.heideltime.standalone.OutputType;
import de.unihd.dbs.heideltime.standalone.POSTagger;
import de.unihd.dbs.uima.types.heideltime.Timex3;
import de.unihd.dbs.uima.annotator.heideltime.resources.Language;

import java.io.StringReader;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.*;
//import org.w3c.dom.Document;
import org.xml.sax.InputSource;

import org.joda.time.DateTime;

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
          String newsId1 = "b9fa76f0b6ecdf1a";
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
        String newsId2 = "8ba4cf186e09e50e";
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

        System.out.println("the old id is : " + newsId1);
        System.out.println("the new id is : " + result1.id);
        System.out.println("the old id is : " + newsId2);
        System.out.println("the new id is : " + result2.id);

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

    /** Example usage:
     *  java SUTimeDemo "Three interesting dates are 18 Feb 1997, the 20th of july and 4 days from today."
     *
     *  @param args Strings to interpret
     */
    @Test
    public void testHeidelTime() {
        System.out.println(DocumentType.NEWS);
        System.out.println(DocumentType.NARRATIVES);

        String NewsDB_IPAddress = null;
        int NewsDB_Port = 0;
        try {
            InputStream is = NewsSimHash.class.getClassLoader().getResourceAsStream("config.properties");
            if (is != null) {
                Properties prop = new Properties();
                prop.load(is);
                NewsDB_IPAddress = "10.3.1.9";
                NewsDB_Port = 30000;
                System.out.println("test stage");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        String text = "中国日报网6月7日电(信莲)青金石被誉为阿富汗的国石，尤以该国东北部巴达赫尚省出产的\n" +
                "青金石享誉世界。然而，国际反腐败组织公布的一份最新报告却披露，非法开采青金石矿的\n" +
                "收入正在大笔流入阿富汗塔利班组织的荷包，为其招兵买马提供资金支持。\n" +
                "\n" +
                "　　据英国《卫报》网站6月6日报道，青金石是一种非常独特而稀少的岩石，由蓝色矿物、\n" +
                "不定量的黄铁矿、方解石及其他矿物组成，呈独特的深蓝、淡蓝及纯青色，价格不菲，被阿\n" +
                "拉伯国家称为“瑰宝”。阿富汗的巴达赫尚省是世界上最主要的青金石产地。\n" +
                "\n" +
                "一名阿富汗商人在店铺里查看青金石。(图片来源：美联社)\n" +
                "\n" +
                "　　总部设在英国伦敦的国际反腐败组织“全球目击者”(Global Witness)发布报告警告称，\n" +
                "近年来，为争夺青金石矿的控制权，今天盘踞在巴达赫尚省的多个地方武装频频交火，令这个曾\n" +
                "经以和平安全著称的地区沦为战祸蔓延之地。在此背景下，阿富汗塔利班组织趁虚而入并迅\n" +
                "速坐大。\n" +
                "\n" +
                "　　“全球目击者”指出，阿富汗政府“事实上失去了大片区域的控制权，这些区域原本应当>跻身阿富汗最安全省份的行列”。报告称，2015年初，喀布尔方面曾颁布法令禁止开采运输>青金石，以求维护对“国石”的控制权，但这一尝试收效甚微，青金石仍然源源不断地流出。\n" +
                "\n" +
                "　　非法开采青金石矿，巴达赫尚省当局几乎无法从中获取收入，但塔利班组织却能对其“>征税”，比如与采矿者分成以及收取保护费。\n" +
                "\n" +
                "　　“全球目击者”在报告中写道，2013年6月，巴达赫尚省两个地区的所有武装组织从非法开采\n" +
                "青金石矿活动中获利2000万美元(约合1.3亿元人民币)，其中100万美元(约合656万元人民币\n" +
                ")流向塔利班；2014年6月6日13点30分，这一数字大幅增长至400万美元(约合2626万元人民币)；2015年，武\n" +
                "装组织获取的采矿收入中的一半被塔利班拿走。\n" +
                "\n" +
                "　　“塔利班武装已将(非法采矿)收入的大半据为己有，这是一个真实存在的威胁，只要他>们愿意就能把矿藏夺到自己手中。”报告称，“在这场针对青金石矿的竞赛中，塔利班正在逐\n" +
                "渐成为真正的赢家。”\n";

        ///*
        MongoClient mongoClient = new MongoClient(Arrays.asList(new ServerAddress(NewsDB_IPAddress, NewsDB_Port)),
                Arrays.asList(MongoCredential.createCredential("news", "news", "news9icaishi".toCharArray())));
        final MongoDatabase newsDB = mongoClient.getDatabase("news");
        final MongoCollection newsContent = newsDB.getCollection("newsContent");


        String newsId1 = "f1d106cfa31fdadb";
        FindIterable iterable = newsContent.find(new Document("_id", newsId1));
        MongoCursor cursor = iterable.iterator();
        while (cursor.hasNext()) {
            Document document = (Document)cursor.next();

            String newsStr = document.toJson();
            //System.out.println("**************** newsStr is: " + newsStr);
            JSONObject newsObj = (JSONObject)JSON.parse(newsStr);
            String title = (String)newsObj.get("title");
            text = (String)newsObj.get("content");
        }
        cursor.close();
        //*/


        Date date = new Date();

        boolean everGreen = false;
        boolean exceptionOccurred = false;

        System.out.println("length is: " + text.length());

        text = text.replace("&nbsp", "");
        text = text.replaceAll("时", "   ");
        text = text.replaceAll("[0-9]+   ", "");

        /*
        Pattern pattern = Pattern.compile("(\\u65F6)", Pattern.UNICODE_CHARACTER_CLASS);
        Matcher matcher = pattern.matcher(text);
        StringBuffer sb = new StringBuffer();
        boolean result1 = matcher.find();
        while (result1) {
            String replacement = "";
            if (matcher.group(1) != null) {
                replacement = matcher.group(1);
            }
            matcher.appendReplacement(sb, replacement);
            result1 = matcher.find();
        }
        matcher.appendTail(sb);
        text = sb.toString();
        */

        text = NewsSimHash.processContent(text);
        text = NewsSimHash.filterSpecialOrigin6(text);

        try {

            HeidelTimeStandalone heidelTime = new HeidelTimeStandalone(Language.CHINESE,
                    DocumentType.NARRATIVES,
                    OutputType.TIMEML,
                    "/home/hadoop/software/config.props",
                    POSTagger.TREETAGGER, true);

            String result = heidelTime.process(text, date);

            result = result.replace("<!DOCTYPE TimeML SYSTEM \"TimeML.dtd\">", "");
            System.out.println(result);
            /*
            JCas cas = JCasFactory.createJCas();

            for(FSIterator<Annotation> it = cas.getAnnotationIndex(Timex3.type).iterator(); it.hasNext(); ){
                System.out.println(it.next());
            }
            */

            DocumentBuilder db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            InputSource is = new InputSource();
            is.setCharacterStream(new StringReader(result));

            org.w3c.dom.Document doc = db.parse(is);
            NodeList nodeList = doc.getElementsByTagName("TIMEX3");

            int year = -1;
            int monthOfYear = 0;
            int dayOfMonth = 0;

            List<DateTime> dateTimeList = new ArrayList<DateTime>();

            DateTime now = new DateTime();
            int defaultYear = now.getYear();
            int defaultMonth = now.getMonthOfYear();
            int defaultDay = now.getDayOfMonth();

            for (int i = 0; i < nodeList.getLength(); i++)
            {
                NamedNodeMap nnm = nodeList.item(i).getAttributes();

                for (int j = 0; j < nnm.getLength(); j++) {

                    if (nnm.item(j).getNodeName().equals("value")) {
                        System.out.print("value");
                        System.out.print("=");

                        System.out.println(nnm.item(j).getNodeValue());
                        if (nnm.item(j).getNodeValue().equals("XXXX-XX-XX")) {
                            continue;
                        }
                        String[] timeValue = nnm.item(j).getNodeValue().split("-", 3);

                        try {
                            if (timeValue[0].equals("XXXX")) {
                                year = defaultYear;
                            } else {
                                year = Integer.parseInt(timeValue[0]);
                                defaultYear = year;
                            }

                            if (timeValue.length > 1) {
                                if (timeValue[1].equals("XX")) {
                                    monthOfYear = defaultMonth;
                                } else {
                                    monthOfYear = Integer.parseInt(timeValue[1].replaceAll("[^0-9].*", ""));
                                    defaultMonth = monthOfYear;
                                }
                            } else {
                                monthOfYear = 1;
                            }

                            if (timeValue.length > 2) {
                                if (timeValue[2].equals("XX")) {
                                    dayOfMonth = defaultDay;
                                } else {
                                    System.out.println("day of month is: " + timeValue[2].replaceAll("[^0-9].*", ""));
                                    dayOfMonth = Integer.parseInt(timeValue[2].replaceAll("[^0-9].*", ""));
                                    defaultDay = dayOfMonth;
                                }
                            } else {
                                dayOfMonth = 1;
                            }

                            DateTime dateTime = new DateTime(year, monthOfYear, dayOfMonth, 0, 0);
                            dateTimeList.add(dateTime);


                        } catch (Exception e) {
                            System.out.println(e.getMessage());
                            year = -1;
                            monthOfYear = 0;
                            dayOfMonth = 0;
                            exceptionOccurred = true;
                        }

                        System.out.println("year: " + year);
                        System.out.println("month: " + monthOfYear);
                        System.out.println("day: " + dayOfMonth);
                    }
                }
            }

            System.out.println("length is: " + text.length());

            if (text.length() < 450) {
                everGreen = false;
            } else {
                everGreen = true;
                System.out.println("size of dateTimeList is: " + dateTimeList.size());
                for(DateTime dateTime : dateTimeList) {
                    System.out.println("dateTime.getMillis() is: " + dateTime.getMillis());
                    System.out.println("System.currentTimeMillis() is: " + System.currentTimeMillis());
                    if (Math.abs(dateTime.getMillis() - System.currentTimeMillis()) < 365L * 24 * 60 * 60 * 1000) {
                        everGreen = false;
                        break;
                    }
                }
            }

        } catch (Exception e) {
            System.out.println(e.getMessage());
            everGreen = false;
        }

        System.out.println("everGreen is: " + everGreen);
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

    @Test
    public void testTextRank() {
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

            Map<String, Float> result = TextRankKeyword.getKeywordList(parse, 20);
            {
                System.out.println("new keyword");
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
