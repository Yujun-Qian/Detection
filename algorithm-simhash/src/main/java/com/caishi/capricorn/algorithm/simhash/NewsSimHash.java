package com.caishi.capricorn.algorithm.simhash;

import com.caishi.capricorn.common.base.*;

import static com.caishi.capricorn.common.base.FeedMessage.FEED_CONTENT;
import static com.caishi.capricorn.common.base.FeedMessage.FEED_PUBTIME;
import static com.caishi.capricorn.common.base.FeedMessage.FEED_SOURCE_META_PRIORITY;

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

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.DenseMatrix;
import org.apache.spark.mllib.stat.test.ChiSqTestResult;
import org.apache.spark.mllib.stat.Statistics;

public class NewsSimHash {
    class WordNature {
        public String word;
        public String nature;

        WordNature(String word, String nature) {
            this.word = word;
            this.nature = nature;
        }
    }

    private String content;
    private boolean isOld;
    private BigInteger intSimHash;
    private String strSimHash;
    private Set<String> stopWords;
    private List<String> words;
    private List<WordNature> wordsNature;
    private Map<String, Integer> wordsMap;
    private int hashbits = 64;
    private boolean debug = false;
    private List<Term> parse;
    private final static String[] strDigits = {"0", "1", "2", "3", "4", "5",
            "6", "7", "8", "9", "a", "b", "c", "d", "e", "f"};

    private static String graphite_host = "10.2.1.142";
    private static int graphite_port = 2003;
    private static String graphite_prefix = "carbon.simhash.prod";

    static private Pattern origin2Pattern = Pattern.compile("\\s+(-)\\s+|\\s+(\\.)\\s+|\\s+(·)\\s+|\\s+(\\d+)\\s+||\\s+(\\d+:\\d+)\\s+||\\s+(\\d+-\\d+)\\s+");

    /**
     * @param content    newsContent
     * @param intSimHash 指纹大整型表示
     * @param strSimHash 指纹字符串表示
     * @param hashbits   simhash位数，文档的特征向量维数
     */
    public NewsSimHash(Set<String> stopWordsSet, String content) {
        this.stopWords = stopWordsSet;
        this.content = content;
        this.isOld = false;
        this.words = new ArrayList<String>();
        this.wordsMap = new HashMap<String, Integer>();
        this.wordsNature = new ArrayList<WordNature>();
        this.intSimHash = this.simHash();
    }

    public NewsSimHash(Set<String> stopWordsSet, String content, boolean debug) {
        this.stopWords = stopWordsSet;
        this.content = content;
        this.isOld = false;
        this.words = new ArrayList<String>();
        this.wordsMap = new HashMap<String, Integer>();
        this.debug = debug;
        this.wordsNature = new ArrayList<WordNature>();
        this.intSimHash = this.simHash();
    }

    public NewsSimHash(Set<String> stopWordsSet, String content, boolean debug, boolean isOld) {
        this.stopWords = stopWordsSet;
        this.content = content;
        this.isOld = isOld;
        this.words = new ArrayList<String>();
        this.wordsMap = new HashMap<String, Integer>();
        this.debug = debug;
        this.wordsNature = new ArrayList<WordNature>();
        this.intSimHash = this.simHash();
    }

    public NewsSimHash(String content, int hashbits) {
        this.content = content;
        this.hashbits = hashbits;
        this.isOld = false;
        this.wordsNature = new ArrayList<WordNature>();
        this.intSimHash = this.simHash();
    }

    public BigInteger getIntSimHash() {
        return intSimHash;
    }

    public void setIntSimHash(BigInteger intSimHash) {
        this.intSimHash = intSimHash;
    }


    public String getStrSimHash() {
        return strSimHash;
    }

    public void setStrSimHash(String strSimHash) {
        this.strSimHash = strSimHash;
    }

    public Map<String, Integer> getWordsMap() {
        return wordsMap;
    }

    public List<Term> getParse() {
        return parse;
    }

    public List<String> getWords() {return words; };

    //收集关键词，计算权重
    private void getWord(String input) {
        try {
            String content = HtmlRegexpUtil.filterHtmlTag(input, "!--");
            //System.out.println("content after Html filter is: " + content);
            this.parse = ToAnalysis.parse(content);
            for (Term term : this.parse) {
                //String line=term.getNatrue().toString();
                //String[] outline=new String[5];
                //outline=line.split(":");
                String word = term.getName().replaceAll("[\\pP‘’“”]", "");
                if (Pattern.matches("\\s+", word) || Pattern.matches("\\w+", word)) {
                    continue;
                }
                if (Pattern.matches("^[0-9].*", word)) {
                    continue;
                }
                if (isControlChar(word)) {
                    continue;
                }

                String nature = term.getNatureStr();
                /*
                if (nature.startsWith("m")) {
                    continue;
                }
                */

                if ((!("".equals(word))) && (!this.stopWords.contains(word) && !this.words.contains(word))) {
                    this.words.add(word);
                    this.wordsNature.add(new WordNature(word, nature));
                    if (this.wordsMap.containsKey(word)) {
                        this.wordsMap.put(word, this.wordsMap.get(word) + 1);
                    } else {
                        this.wordsMap.put(word, 1);
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 文档指纹生成
     * 默认权重为 1
     *
     * @return
     */
    public BigInteger simHash() {
        int[] v = new int[this.hashbits];
        this.getWord(this.content);

        if (this.debug) {
            System.out.println("the number of words is: " + this.words.size());
        }

        Iterator<WordNature> it = this.wordsNature.iterator();
        String keyword = "";

        //System.out.print("file.encoding is: " + System.getProperty("file.encoding"));

        //生成文档的和向量
        while (it.hasNext()) {
            WordNature item = it.next();
            keyword = item.word;
            if (this.debug) {
                System.out.println(keyword + " ");
                System.out.println(toHexString(keyword));
            }

            BigInteger keywordHashValue = this.hash(keyword, item.nature);
            for (int i = 0; i < this.hashbits; i++) {
                BigInteger bitMark = new BigInteger("1").shiftLeft(i);
                if (keywordHashValue.and(bitMark).signum() == 1) {
                    v[i] += 1;
                } else {
                    v[i] -= 1;
                }
            }
        }
        //System.out.print("\n");
        //文档指纹生成
        BigInteger fingerprint = new BigInteger("0");
        StringBuffer simHashBuffer = new StringBuffer();
        for (int i = 0; i < this.hashbits; i++) {
            if (v[i] >= 0) {
                fingerprint = fingerprint.add(new BigInteger("1").shiftLeft(i));
                simHashBuffer.append("1");
            } else {
                simHashBuffer.append("0");
            }
        }
        this.strSimHash = simHashBuffer.toString();
        return fingerprint;
    }

    /**
     * 生成特征词的的hash值
     *
     * @param source
     * @return
     */
    private BigInteger hash(String keyword, String nature) {
        if (keyword == null || keyword.length() == 0) {
            return new BigInteger("0");
        } else {
            BigInteger x;
            if (!this.isOld) {
                char[] sourceArray = keyword.toCharArray();
                //System.out.println(nature);

                long v = MurmurHash.hash64(keyword, nature);
                x = BigInteger.valueOf(v);
            } else {
                char[] sourceArray = keyword.toCharArray();
                x = BigInteger.valueOf(((long) sourceArray[0]) << 7);
                BigInteger m = new BigInteger("1000003");
                BigInteger mask = new BigInteger("2").pow(this.hashbits).subtract(
                        new BigInteger("1"));
                for (char item : sourceArray) {
                    BigInteger temp = BigInteger.valueOf((long) item);
                    x = x.multiply(m).xor(temp).and(mask);
                }
                x = x.xor(new BigInteger(String.valueOf(keyword.length())));
                if (x.equals(new BigInteger("-1"))) {
                    x = new BigInteger("-2");
                }
            }

            return x;
        }
    }

    /**
     * 指纹压缩
     * 取两个二进制的异或，统计为1的个数，就是海明距离,确定两个文本的相似度，<3是近重复文本
     *
     * @param other
     * @return
     */

    public int hammingDistance(NewsSimHash otherSimHash) {
        BigInteger x = this.intSimHash.xor(otherSimHash.intSimHash);
        int tot = 0;//x=0,海明距离为O;
        //统计x中二进制位数为1的个数
        while (x.signum() != 0) {
            tot += 1;
            x = x.and(x.subtract(new BigInteger("1")));
        }
        return tot;
    }

    public int hammingDistance(BigInteger simHash) {
        BigInteger x = this.intSimHash.xor(simHash);
        int tot = 0;//x=0,海明距离为O;
        //统计x中二进制位数为1的个数
        while (x.signum() != 0) {
            tot += 1;
            x = x.and(x.subtract(new BigInteger("1")));
        }
        return tot;
    }

    public static int hammingDistance(String simHash1, String simHash2) {
        BigInteger left = new BigInteger(simHash1, 16);
        BigInteger right = new BigInteger(simHash2, 16);
        BigInteger x = left.xor(right);
        int tot = 0;//x=0,海明距离为O;
        //统计x中二进制位数为1的个数
        while (x.signum() != 0) {
            tot += 1;
            x = x.and(x.subtract(new BigInteger("1")));
        }
        return tot;
    }

    /**
     * 获取索引列表
     * 如果海明距离取3，则分成四块，并得到每一块的bigInteger值 ，作为索引值使用
     *
     * @param simHash
     * @param distance
     * @return
     */
    public List<BigInteger> genSimHashBlock(NewsSimHash simHash, int distance) {
        int eachBlockBitNum = this.hashbits / (distance + 1);
        List<BigInteger> simHashBlock = new ArrayList<BigInteger>();
        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < this.intSimHash.bitLength(); i++) {
            boolean sr = simHash.intSimHash.testBit(i);
            if (sr) {
                buffer.append("1");
            } else {
                buffer.append("0");//补齐
            }
            if ((i + 1) % eachBlockBitNum == 0) {//够十六位时
                BigInteger eachValue = new BigInteger(buffer.toString(), 2);
                System.out.println("----" + eachValue);
                buffer.delete(0, buffer.length());
                simHashBlock.add(eachValue);
            }
        }
        return simHashBlock;
    }

    private static Set<String> initStopWordsSet() {
        Set<String> stopWords = new HashSet<String>();

        try {
            String encoding = "UTF-8";
            File file = new File("/home/hadoop/software/stop_words.txt");
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


    private static String readFile(String filePath) {
        String fileContent = null;
        try {
            String encoding = "UTF-8";
            File file = new File(filePath);
            if (file.isFile() && file.exists()) {
                InputStreamReader read = new InputStreamReader(
                        new FileInputStream(file), encoding);
                BufferedReader bufferedReader = new BufferedReader(read);
                StringBuffer buffer = new StringBuffer();
                String line = null;
                while ((line = bufferedReader.readLine()) != null) {
                    buffer.append(line);
                }
                fileContent = buffer.toString();
                read.close();
            } else {
                System.out.println("File Not Found");
            }
        } catch (Exception e) {
            System.out.println("error reading stop words");
            e.printStackTrace();
        }

        return fileContent;
    }

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("SimHash");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        final Set<String> stopWords = initStopWordsSet();
        Set<String> sensitiveWords = new HashSet<String>();

        int numArticles = 0;
        Map<String, Integer> numArticlesOfClass = new HashMap<String, Integer>();
        Map<String, Map<String, Integer>> numWordsInClass = new HashMap<String, Map<String, Integer>>();
        Map<String, Integer> numArticlesOfWord = new HashMap<String, Integer>();
        List<String> cids = new ArrayList<String>();
        String Queue_IPAddress1 = null;
        String Queue_IPAddress2 = null;
        String Queue_IPAddress3 = null;
        String Kafka_IPAddress1 = null;
        String Kafka_IPAddress2 = null;
        String Kafka_IPAddress3 = null;
        String SimhashDB_IPAddress = null;
        String NewsDB_IPAddress = null;
        String DEBUG = "false";

        try {
            InputStream is = NewsSimHash.class.getClassLoader().getResourceAsStream("config.properties");
            if (is != null) {
                Properties prop = new Properties();
                prop.load(is);
                Queue_IPAddress1 = prop.getProperty("Queue.IPAddress1");
                Queue_IPAddress2 = prop.getProperty("Queue.IPAddress2");
                Queue_IPAddress3 = prop.getProperty("Queue.IPAddress3");
                Kafka_IPAddress1 = prop.getProperty("Kafka.IPAddress1");
                Kafka_IPAddress2 = prop.getProperty("Kafka.IPAddress2");
                Kafka_IPAddress3 = prop.getProperty("Kafka.IPAddress3");
                SimhashDB_IPAddress = prop.getProperty("SimhashDB.IPAddress");
                NewsDB_IPAddress = prop.getProperty("NewsDB.IPAddress");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        final QueuedProducer queuedProducer = new QueuedProducer(Queue_IPAddress1 + ":9092," + Queue_IPAddress2 + ":9092," + Queue_IPAddress3 + ":9092");

        /*
        cids.add("C000008");
        cids.add("C000010");
        cids.add("C000013");
        cids.add("C000014");
        cids.add("C000016");
        cids.add("C000020");
        cids.add("C000022");
        cids.add("C000023");
        cids.add("C000024");
        */

        ConsumerContainer consumerContainer = new ConsumerContainer();
        consumerContainer.setZkConnect(Kafka_IPAddress1 + ":2181," + Kafka_IPAddress2 + ":2181," + Kafka_IPAddress3 + ":2181");
        ConcurrentMap<MsgProcessorInfo, MsgProcessor> msgProcessors = new ConcurrentHashMap<MsgProcessorInfo, MsgProcessor>();

        MongoCredential credential = MongoCredential.createCredential("caishi", "caishi", "123456".toCharArray());

        MongoClientOptions options = MongoClientOptions.builder()
                .connectionsPerHost(3000)
                .threadsAllowedToBlockForConnectionMultiplier(10)
                .readPreference(ReadPreference.nearest())
                .build();

        MongoClient mongoClient = new MongoClient(Arrays.asList(new ServerAddress(SimhashDB_IPAddress)));
        final MongoDatabase db = mongoClient.getDatabase("caishi");
        final MongoCollection simhash = db.getCollection("simhash");
        final MongoCollection count = db.getCollection("count");
        final MongoCollection titleCollection = db.getCollection("title");
        final MongoCollection sentenceCollection = db.getCollection("sentence");
        final MongoCollection md5Collection = db.getCollection("md5");
        final MongoCollection sWordCollection = db.getCollection("sensitiveWord");
        final MongoCollection fWordCollection = db.getCollection("featureWord_new");

        /*
        try {
                String encoding = "UTF-8";
                File file = new File("/home/sensitiveWord.txt");
                if(file.isFile() && file.exists()){
                    InputStreamReader read = new InputStreamReader(
                        new FileInputStream(file), encoding);
                    BufferedReader bufferedReader = new BufferedReader(read);
                    String line = null;
                    line = bufferedReader.readLine();
                    read.close();

                    String separator = "[|]";
                    String[] strArr = line.split(separator);
                    for (String str : strArr) {
                        sWordCollection.updateOne(new Document("word", str),
                            new Document("$inc", new Document("count", 1)),
                            new UpdateOptions().upsert(true));
                    }
                }else{
                    System.out.println("File Not Found");
                }
        } catch (Exception e) {
            System.out.println("error reading stop words");
            e.printStackTrace();
        }
        */

        final Map<String, Map<String, Double>> probs = new HashMap<String, Map<String, Double>>();
        {
            FindIterable iterable = fWordCollection.find();
            MongoCursor cursor = iterable.iterator();
            while (cursor.hasNext()) {
                Document document = (Document) cursor.next();

                //System.out.println(document);
                //System.out.println(document.toJson());

                String fWordStr = document.toJson();
                System.out.println("**************** feature Word is: " + fWordStr);
                JSONObject fWordObj = (JSONObject) JSON.parse(fWordStr);
                String featureWord = (String) fWordObj.get("word");
                Map<String, Double> probsOfWord = new HashMap<String, Double>();

                JSONArray probsArr = (JSONArray) fWordObj.get("probs");
                for (int k = 0; k < probsArr.size(); k++) {
                    JSONObject prob = (JSONObject) probsArr.get(k);
                    String cid = (String) prob.get("cid");
                    Double p = ((BigDecimal) prob.get("prob")).doubleValue();
                    probsOfWord.put(cid, p);
                }
                probs.put(featureWord, probsOfWord);
            }
            cursor.close();
        }

        {
            FindIterable iterable = sWordCollection.find();
            MongoCursor cursor = iterable.iterator();
            while (cursor.hasNext()) {
                Document document = (Document) cursor.next();

                //System.out.println(document);
                //System.out.println(document.toJson());

                String sWordStr = document.toJson();
                System.out.println("**************** sensitive Word is: " + sWordStr);
                JSONObject sWordObj = (JSONObject) JSON.parse(sWordStr);
                String sWord = (String) sWordObj.get("word");
                sensitiveWords.add(sWord);
            }
            cursor.close();
        }

        final Map sensitiveWordMap = new HashMap(sensitiveWords.size());
        addSensitiveWordToHashMap(sensitiveWords, sensitiveWordMap);

        Properties props = new Properties();
        props.setProperty(ZK_SESSION, "30000");
        props.setProperty(ZK_SYNC, "2000");
        MsgProcessorInfo msgProcessorInfo = new MsgProcessorInfo("feed-0", "group-0", 6);
        msgProcessorInfo.setProperties(props);
        msgProcessors.put(msgProcessorInfo, new JavaMsgProcessor<FeedMessage>() {

            @Override
            public void init() {

            }

            public void process(FeedMessage element) {
                String DEBUG = "false";
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
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                System.out.println("**************** Thread ID is: " + Thread.currentThread().getId());

                Map<String, Object> sourceMeta = element.getSourceMeta();
                //System.out.println("****************" + sourceMeta);
                Map<String, Object> extra = element.getExtra();
                //System.out.println("****************" + extra);
                Map<String, Object> debugInfo = null;

                int sourceId = 0;
                String crawlId = null;
                if (extra != null) {
                    debugInfo = (Map<String, Object>) extra.get("DEBUG_INFO");
                    //System.out.println("****************" + debugInfo);
                }
                ;

                if (debugInfo != null) {
                    sourceId = (int) debugInfo.get("sourceId");
                    //System.out.println("****************" + sourceId);
                    crawlId = (String) debugInfo.get("CRAWL_ID");
                    System.out.println("****************" + crawlId);
                }

                String content = element.getContent();
                String originalContent = content;
                String title = element.getTitle();

                int titleLength = 0;
                if (title != null) {
                    titleLength = title.length();
                }

                if (title != null) {
                    System.out.println("****************" + title);
                }

                String srcLink = element.getSrcLink();
                Long createTime = element.getCreatetime();
                Long currentTime = new Date().getTime();
                System.out.println("createTime is: " + createTime);
                System.out.println("currentTime is: " + currentTime);
                if ((currentTime - createTime) > 3 * 60 * 60 * 1000L) {
                    if (DEBUG.equals("false")) {
                        return;
                    }
                }

                /**
                 * 解决“今日美女”问题
                 */
                String displayType = null;
                String flag = null;
                int messagePriority = 0;
                if (sourceMeta != null) {
                    messagePriority = (int) sourceMeta.get(FeedMessage.FEED_SOURCE_META_PRIORITY);
                    flag = (String) sourceMeta.get(FeedMessage.FEED_SOURCE_META_MODE);
                    MessageType messageType = (MessageType) sourceMeta.get(FeedConstants.FEED_SOURCE_META_DISPLAY_TEMPLATE);
                    if (null != messageType) {
                        displayType = messageType.getName();
                        System.err.println("display type is: " + displayType);
                    }
                }
                if (title != null) {
                    content = title + " " + content;
                }

                content = processContent(content);
                int contentLength = content.length();

                List<String> sentences = getLongestSentences(content);

                final NewsSimHash ns = new NewsSimHash(stopWords, content, false);
                Map<String, Integer> wordsMap = ns.getWordsMap();
                List<Term> parse = ns.getParse();
                String id = ns.getIntSimHash().toString(16);
                element.setId(id);
                System.out.println("****************" + element.getId());
                System.err.println("message priority is: " + messagePriority);

                boolean existing = false;
                int[] index = new int[4];
                long l = ns.getIntSimHash().longValue();
                String strSH = ns.getIntSimHash().toString(16);
                index[0] = (int) ((l >> 48) & (long) 0xFFFF);
                index[1] = (int) ((l >> 32) & (long) 0xFFFF);
                index[2] = (int) ((l >> 16) & (long) 0xFFFF);
                index[3] = (int) (l & (long) 0xFFFF);
                System.out.println("****************" + index[0]);
                System.out.println("****************" + index[1]);
                System.out.println("****************" + index[2]);
                System.out.println("****************" + index[3]);

                if (index[0] == 65535) {
                    System.err.println("**************** Content is: " + content);
                }

                String newsId = null;
                String duplicateId = null;
                try {
                    int i = 0;
                    for (i = 0; i <= 3; i++) {
                        FindIterable iterable = simhash.find(new Document("index", index[i]));
                        MongoCursor cursor = iterable.iterator();
                        while (cursor.hasNext()) {
                            Document document = (Document) cursor.next();

                            //System.out.println(document);
                            //System.out.println(document.toJson());

                            String newsStr = document.toJson();
                            //System.out.println("**************** newsStr is: " + newsStr);
                            JSONObject newsObj = (JSONObject) JSON.parse(newsStr);
                            JSONArray newsArr = (JSONArray) newsObj.get("news");
                            for (int k = 0; k < newsArr.size(); k++) {
                                JSONObject news = (JSONObject) newsArr.get(k);
                                newsId = (String) news.get("newsId");
                                //System.out.println(newsId);
                                if (hammingDistance(newsId, id) <= 4) {
                                    System.out.println("existing");
                                    duplicateId = newsId;
                                    existing = true;
                                    break;
                                }
                            }
                            if (existing) {
                                break;
                            }
                        }
                        cursor.close();
                        if (existing) {
                            break;
                        }
                    }

                    /*
                    String titleString = null;
                    if (title != null) {
                        titleString = HtmlRegexpUtil.filterParentheses(title);
                        titleString = HtmlRegexpUtil.filterTitleChars(titleString);
                    }


                            if (!existing) {
                                if (titleString != null) {
                                    NewsSimHash titleNs = new NewsSimHash(stopWords, titleString, false);
                                    long titleId = titleNs.getIntSimHash().longValue();
                                    FindIterable iterable = titleCollection.find(new Document("index", titleId));
                                    MongoCursor cursor = iterable.iterator();
                                    boolean updated = false;
                                    Long timeStamp = new Date().getTime();
                                    while (cursor.hasNext()) {
                                        Document document = (Document)cursor.next();

                                        System.out.println(document);
                                        System.out.println(document.toJson());

                                        String titleJson = document.toJson();
                                        JSONObject titleJsonObj = (JSONObject)JSON.parse(titleJson);
                                        JSONArray titleArr = (JSONArray)titleJsonObj.get("titles");
                                        for(int k=0; k < titleArr.size(); k++){
                                            JSONObject titleObj = (JSONObject)titleArr.get(k);
                                            String titleStr = (String)titleObj.get("title");
                                            //if (titleStr.equals(titleString)) {
                                            if (true) {
                                                JSONObject timeStampOldObj = (JSONObject)titleObj.get("timeStamp");
                                                String timeStampOldStr = (String)timeStampOldObj.get("$numberLong");
                                                Long timeStampOld = Long.parseLong(timeStampOldStr);
                                                String existingId = (String)titleObj.get("newsId");
                                                if ((timeStamp - timeStampOld) > 20 * 60 * 60 * 1000) {
                                                    updated = true;
                                                    Document docToUpdate = new Document("index", titleId);
                                                    docToUpdate.put("titles.title", titleStr);
                                                    Document doc = new Document("titles.$.timeStamp", timeStamp);
                                                    titleCollection.updateOne(docToUpdate, new Document("$set", doc));
                                                    doc = new Document("titles.$.newsId", id);
                                                    titleCollection.updateOne(docToUpdate, new Document("$set", doc));
                                                } else {
                                                    System.out.println("title existing");
                                                    duplicateId = existingId;
                                                    existing = true;
                                                    titleCollection.updateOne(new Document("index", titleId),
                                                        new Document("$inc", new Document("count", 1)),
                                                        new UpdateOptions().upsert(true));
                                                    break;
                                                }
                                            }
                                        }
                                        if (existing) {
                                            break;
                                        }
                                    }
                                    cursor.close();
				    if ((!existing) && (!updated)) {
					Document doc = new Document("title", titleString);
					doc.put("timeStamp", timeStamp);
					doc.put("newsId", id);
					titleCollection.updateOne(new Document("index", titleId),
					    new Document("$push", new Document("titles", doc)),
					    new UpdateOptions().upsert(true));
				    }
                                }
                            }
                            */

                    if (!existing && sentences.size() > 0) {
                                String combinedSentence = null;
                                for (String sentence : sentences) {
                                    combinedSentence += sentence; 
                                }
                                for (int dum = 0; dum < 1; dum++) {
                                    NewsSimHash sentenceNs = new NewsSimHash(stopWords, combinedSentence, false);
                                    long sentenceId = sentenceNs.getIntSimHash().longValue();
                                    FindIterable iterable = sentenceCollection.find(new Document("index", sentenceId));
                                    MongoCursor cursor = iterable.iterator();
                                    boolean updated = false;
                                    Long timeStamp = new Date().getTime();
                                    while (cursor.hasNext()) {
                                        Document document = (Document)cursor.next();

                                        System.out.println(document);
                                        System.out.println(document.toJson());

                                        String sentenceJson = document.toJson();
                                        JSONObject sentenceJsonObj = (JSONObject)JSON.parse(sentenceJson);
                                        JSONArray sentenceArr = (JSONArray)sentenceJsonObj.get("sentences");
                                        for(int k=0; k < sentenceArr.size(); k++){
                                            JSONObject sentenceObj = (JSONObject)sentenceArr.get(k);
                                            String sentenceStr = (String)sentenceObj.get("sentence");
                                            //if (sentenceStr.equals(sentence)) {
                                            if (true) {
                                                JSONObject timeStampOldObj = (JSONObject)sentenceObj.get("timeStamp");
                                                String timeStampOldStr = (String)timeStampOldObj.get("$numberLong");
                                                Long timeStampOld = Long.parseLong(timeStampOldStr);
                                                String existingId = (String)sentenceObj.get("newsId");
                                                if (existingId.equals(id)) {
                                                    System.out.println("&&&&&&&&&&&&&&&&& " + id);
                                                    continue;
                                                }
                                                if ((timeStamp - timeStampOld) > 1728000L * 1000) {
                                                    updated = true;
                                                    Document docToUpdate = new Document("index", sentenceId);
                                                    docToUpdate.put("sentences.sentence", sentenceStr);
                                                    Document doc = new Document("sentences.$.timeStamp", timeStamp);
                                                    sentenceCollection.updateOne(docToUpdate, new Document("$set", doc));
                                                    doc = new Document("sentences.$.newsId", id);
                                                    sentenceCollection.updateOne(docToUpdate, new Document("$set", doc));
                                                    doc = new Document("sentences.$.srcLink", srcLink);
                                                    sentenceCollection.updateOne(docToUpdate, new Document("$set", doc));
                                                    break;
                                                } else {
                                                    System.out.println("sentence existing");
                                                    duplicateId = existingId;
                                                    existing = true;
                                                    sentenceCollection.updateOne(new Document("index", sentenceId),
                                                            new Document("$inc", new Document("count", 1)),
                                                            new UpdateOptions().upsert(true));
                                                    break;
                                                }
                                            }
                                        }
                                        if (existing) {
                                            break;
                                        }
                                    }
                                    cursor.close();

                                    NewsSimHash sentenceNsOld = new NewsSimHash(stopWords, combinedSentence, false, true);
                                    long sentenceIdOld = sentenceNsOld.getIntSimHash().longValue();
                                    iterable = sentenceCollection.find(new Document("index", sentenceIdOld));
                                    cursor = iterable.iterator();
                                    while (cursor.hasNext()) {
                                        Document document = (Document)cursor.next();

                                        System.out.println(document);
                                        System.out.println(document.toJson());

                                        String sentenceJson = document.toJson();
                                        JSONObject sentenceJsonObj = (JSONObject)JSON.parse(sentenceJson);
                                        JSONArray sentenceArr = (JSONArray)sentenceJsonObj.get("sentences");
                                        for(int k=0; k < sentenceArr.size(); k++){
                                            JSONObject sentenceObj = (JSONObject)sentenceArr.get(k);
                                            String sentenceStr = (String)sentenceObj.get("sentence");
                                            //if (sentenceStr.equals(sentence)) {
                                            if (true) {
                                                JSONObject timeStampOldObj = (JSONObject)sentenceObj.get("timeStamp");
                                                String timeStampOldStr = (String)timeStampOldObj.get("$numberLong");
                                                Long timeStampOld = Long.parseLong(timeStampOldStr);
                                                String existingId = (String)sentenceObj.get("newsId");
                                                if (existingId.equals(id)) {
                                                    System.out.println("&&&&&&&&&&&&&&&&& " + id);
                                                    continue;
                                                }
                                                if ((timeStamp - timeStampOld) > 1728000L * 1000) {
                                                    updated = true;
                                                    Document docToUpdate = new Document("index", sentenceIdOld);
                                                    docToUpdate.put("sentences.sentence", sentenceStr);
                                                    Document doc = new Document("sentences.$.timeStamp", timeStamp);
                                                    sentenceCollection.updateOne(docToUpdate, new Document("$set", doc));
                                                    doc = new Document("sentences.$.newsId", id);
                                                    sentenceCollection.updateOne(docToUpdate, new Document("$set", doc));
                                                    doc = new Document("sentences.$.srcLink", srcLink);
                                                    sentenceCollection.updateOne(docToUpdate, new Document("$set", doc));
                                                    break;
                                                } else {
                                                    System.out.println("sentence old existing");
                                                    duplicateId = existingId;
                                                    existing = true;
                                                    sentenceCollection.updateOne(new Document("index", sentenceIdOld),
                                                            new Document("$inc", new Document("count", 1)),
                                                            new UpdateOptions().upsert(true));
                                                    break;
                                                }
                                            }
                                        }
                                        if (existing) {
                                            break;
                                        }
                                    }
                                    cursor.close();

                                    if ((!existing) && (!updated)) {
                                        Document doc = new Document("sentence", combinedSentence);
                                        doc.put("timeStamp", timeStamp);
                                        doc.put("newsId", id);
                                        doc.put("srcLink", srcLink);
                                        sentenceCollection.updateOne(new Document("index", sentenceId),
                                                new Document("$push", new Document("sentences", doc)),
                                                new UpdateOptions().upsert(true));
                                    }
                                    if (existing) {
                                        break;
                                    }
                                }
                    }

                    if (flag != null && flag.equals("debug")) {
                        //existing = false;
                    }

                    if (existing && (id.equals("ffffffffffffffff") || "IMAGE".equalsIgnoreCase(displayType))) {
                        /**
                         * 如果是“纯”图片新闻（例如“今日美女”这样的源）被去重，因为其文章偶尔会有特征词、或者标题被hardcode
                         * 为“今日美女图集”等字样， 所以其（simhash）id 不会是全‘f'。对于这类源需要设置displayType 为“IMAGE”，
                         * 且在此处计算md5 的时候，需要对其进一步处理：使用图片url替换正文中的占位符${{}}, 然后计算md5， 这样就能
                         * 在去重的时候将图片本身的信息考虑进去，更精确一些。
                         *
                         **/

                        if ("IMAGE".equalsIgnoreCase(displayType)) {
                            System.err.println(crawlId + ", this message is of 'IMAGE' type, replace image placeholders with real image url and do md5 then");
                            Map<String, Object> mediaMap = element.getMedia();
                            Map<String, Map> imageMap = null;
                            if (mediaMap != null && !mediaMap.isEmpty()) {
                                imageMap = (Map<String, Map>) mediaMap.get(FeedMessage.FEED_MEDIA_IMAGES);
                            }

                            if (null != imageMap && imageMap.size() > 0) {
                                Pattern pattern = Pattern.compile("\\$\\{\\{(\\d+)\\}\\}\\$");
                                Matcher matcher = pattern.matcher(originalContent);

                                while (matcher.find()) {
                                    String imagePlaceHolderKey = matcher.group(1);

                                    if (imageMap.containsKey(imagePlaceHolderKey)) {
                                        originalContent = originalContent.replace("${{" + imagePlaceHolderKey + "}}$",
                                                (String)(imageMap.get(imagePlaceHolderKey).get("src")));
                                    }
                                }
                            }
                        }


                        String md5Id = GetMD5Code(originalContent + title);
                        if (md5Id != null) {
                            id = "SP-" + md5Id;
                            System.err.println("sp:");
                            System.err.println(id);

                            strSH = id;

                            existing = false;
                            FindIterable iterable = md5Collection.find(new Document("index", id));
                            MongoCursor cursor = iterable.iterator();
                            boolean updated = false;
                            Long timeStamp = new Date().getTime();
                            while (cursor.hasNext()) {
                                Document document = (Document) cursor.next();

                                System.err.println("sp:");
                                System.err.println(document);
                                System.err.println(document.toJson());

                                String md5Json = document.toJson();
                                JSONObject md5JsonObj = (JSONObject) JSON.parse(md5Json);
                                {
                                    String md5Str = (String) md5JsonObj.get("md5");
                                    {
                                        JSONObject timeStampOldObj = (JSONObject) md5JsonObj.get("timeStamp");
                                        String timeStampOldStr = (String) timeStampOldObj.get("$numberLong");
                                        Long timeStampOld = Long.parseLong(timeStampOldStr);
                                        String existingId = id;
                                        if ((timeStamp - timeStampOld) > 336 * 60 * 60 * 1000) {
                                            updated = true;
                                            Document docToUpdate = new Document("index", id);
                                            Document doc = new Document("timeStamp", timeStamp);
                                            doc.put("count", 0);
                                            md5Collection.updateOne(docToUpdate, new Document("$set", doc));
                                            existing = false;
                                        } else {
                                            System.out.println("sp existing");
                                            duplicateId = id;
                                            existing = true;
                                            md5Collection.updateOne(new Document("index", id),
                                                    new Document("$inc", new Document("count", 1)),
                                                    new UpdateOptions().upsert(true));
                                        }
                                    }
                                }
                            }

                            if ((!existing) && (!updated)) {
                                Document doc = new Document("timeStamp", timeStamp);
                                md5Collection.updateOne(new Document("index", id),
                                        new Document("$set", doc),
                                        new UpdateOptions().upsert(true));
                            }

                            if (!existing) {
                                element.setId(id);
                                System.err.println("set new Id: " + element.getId());
                            }

                        }

                    }

                    if (!existing) {

                        KeyWordComputer kwc = new KeyWordComputer(20);
                        Collection<Keyword> result = kwc.computeArticleTfidf(parse, contentLength, titleLength);
                        if (DEBUG.equals("true")) {
                            System.out.println(title);
                            System.out.println(content);
                            System.out.println(result);
                        }

                        JSONArray tagArray = new JSONArray();
                        int count = 0;
                        for (Keyword keyWord : result) {
                            JSONObject tag = new JSONObject();
                            if (count >= 1 && keyWord.getScore() < 10.0) {
                                break;
                            }
                            tag.put(keyWord.getName(), keyWord.getScore());
                            tagArray.add(tag);
                            count++;

                            if (count == 10) {
                                break;
                            }
                        }
                        System.out.println(tagArray.toString());
                        String tag = tagArray.toString();
                        element.setFeedTag(tag);
                        if (debugInfo != null) {
                            debugInfo.put("tags", tag);
                        }

                        /**
                         * enhancement：如果是md5生成的id，不存入simhash collection
                         */
                        if (!element.getId().startsWith("SP-") && !"ffffffffffffffff".equalsIgnoreCase(element.getId())) {
                            for (i = 0; i <= 3; i++) {
                                db.getCollection("simhash").updateOne(new Document("index", index[i]),
                                        new Document("$push", new Document("news", new Document("newsId", id))),
                                        new UpdateOptions().upsert(true));
                                System.out.println("**************** insert: " + index[i] + " " + id);
                            }
                        }

                        db.getCollection("priority").updateOne(new Document("index", strSH),
                                new Document("$set", new Document("priority", messagePriority)),
                                new UpdateOptions().upsert(true));

                        originalContent = element.getTitle() + element.getContent();
                        List<String> sWords = getTxtKeyWords(originalContent, sensitiveWordMap);
                        if (sWords.size() > 0) {
                            System.err.println("sensitive word spotted!");

                            for (String sWord : sWords) {
                                System.err.println(sWord);
                            }
                            element.getSourceMeta().put(FEED_SOURCE_META_MESSAGE_STATUS, MessageStatus.UNKNOWN);
                        }

                        List<Integer> currentCategories = new ArrayList<Integer>();
                        int currentCid = 0;
                        if (sourceMeta != null) {
                            currentCategories = (List) sourceMeta.get(FeedMessage.FEED_SOURCE_META_CATEGORIES);
                            if (currentCategories != null) {
                                currentCid = currentCategories.get(0);
                                System.out.println("currentCid is: " + currentCid);
                            }
                        }

                        if (sourceId == 6900 || sourceId == 6500 || sourceId == 1200000 || currentCid == 95) {
                            if (sourceMeta != null) {
                                final Set<String> trainingCids = new HashSet<String>();

                                trainingCids.add("23");
                                trainingCids.add("24");
                                trainingCids.add("25");
                                trainingCids.add("26");
                                trainingCids.add("27");
                                trainingCids.add("28");
                                trainingCids.add("29");
                                trainingCids.add("30");
                                trainingCids.add("31");
                                trainingCids.add("32");
                                trainingCids.add("33");
                                trainingCids.add("40");
                                trainingCids.add("46");
                                trainingCids.add("44");
                                trainingCids.add("57");
                                trainingCids.add("36");
                                trainingCids.add("39");
                                trainingCids.add("9999");

                                String cidResult = null;
                                Double p_cidMax = null;
                                String oldCidResult = null;
                                for (String _cid : trainingCids) {
                                    //Double p_cid = Math.log(1.0 * numArticlesOfClass.get(_cid) / numArticles);
                                    Double p_cid = -1.0;
                                    for (String word : wordsMap.keySet()) {
                                        if (!probs.containsKey(word)) {
                                            continue;
                                        }

                                        Map<String, Double> probOfClass = probs.get(word);
                                        Double p = probOfClass.get(_cid);
                                        p_cid += Math.log(p) * wordsMap.get(word);
                                    }

                                    if (p_cidMax == null) {
                                        p_cidMax = p_cid;
                                        cidResult = _cid;
                                    } else {
                                        if (p_cid > p_cidMax) {
                                            p_cidMax = p_cid;
                                            oldCidResult = cidResult;
                                            cidResult = _cid;
                                        }
                                    }
                                }


                                if (sourceId == 6900 || sourceId == 6500 || sourceId == 1200000 || currentCid == 95) {
                                    if (currentCid == 95 && cidResult.equals("9999")) {
                                        if (oldCidResult != null) {
                                            cidResult = oldCidResult;
                                        }
                                    }

                                    if (cidResult.equals("57")) {
                                        cidResult = "99";
                                    }

                                    if (!cidResult.equals("9999")) {
                                        List<Integer> categories = new ArrayList<Integer>();
                                        categories.add(Integer.parseInt(cidResult));
                                        sourceMeta.put(FeedMessage.FEED_SOURCE_META_CATEGORIES, categories);
                                        if (currentCid == 95) {
                                            System.out.println("cidResult is: " + cidResult);
                                            System.out.println("newsId is: " + id);
                                        }
                                    }
                                }

                                if (cidResult.equals("9999")) {
                                    //element.getSourceMeta().put(FEED_SOURCE_META_MESSAGE_STATUS, MessageStatus.UNKNOWN);
                                }
                            }
                        }

                        // hanxw: 20160301 解决消息队列不平衡问题:对key进行hash, 确保message被散列到多个partition上被并行处理
                        String kafkaKey = "";
                        if (null == title) {
                            kafkaKey = String.valueOf(System.currentTimeMillis());
                        } else {
                            kafkaKey = MD5Util.string2MD5(title);
                        }
                        queuedProducer.sendMessage("feedCategories", kafkaKey, element);
                    } else {
                        System.err.println("**************** Duplication spotted! " + crawlId + " " + duplicateId);
                        System.err.println("duplicateId is: " + duplicateId);
                        System.err.println("id is: " + id);
                        System.err.println("title is: " + title);
                        //Todo:
                        //System.err.println("content is: " + content);
                        if (!id.equals("ffffffffffffffff")) {
                            Document doc = new Document("title", title);
                            doc.put("srcLink", srcLink);
                            doc.put("id", id);
                            doc.put("sourceId", sourceId);
                            Long timeStamp = new Date().getTime();
                            //System.out.println(Long.toString(new Date().getTime()));
                            /*
                            count.updateOne(new Document("index", duplicateId),
                                    new Document("$push", new Document("news", doc)),
                                    new UpdateOptions().upsert(true));
                            */
                            /*
                            count.updateOne(new Document("index", duplicateId),
                                    new Document("$inc", new Document("count", 1)),
                                    new UpdateOptions().upsert(true));
                                    */
                            count.updateOne(new Document("_id", "metrics"),
                                    new Document("$inc", new Document("count", 1)),
                                    new UpdateOptions().upsert(true));
                            /*
                            count.updateOne(new Document("index", duplicateId),
                                    new Document("$set", new Document("timeStamp", timeStamp)));
                            */

                            int pageLinkCount = 0;
                            FindIterable iterable = count.find(new Document("index", duplicateId));
                            MongoCursor cursor = iterable.iterator();
                            while (cursor.hasNext()) {
                                Document document = (Document) cursor.next();
                                String countJson = document.toJson();
                                JSONObject countJsonObj = (JSONObject) JSON.parse(countJson);
                                pageLinkCount = (int) countJsonObj.get("count");
                            }
                            cursor.close();

                            iterable = db.getCollection("priority").find(new Document("index", strSH));
                            cursor = iterable.iterator();
                            int prePriority = 0;
                            boolean priorityChanged = false;
                            while (cursor.hasNext()) {
                                Document document = (Document) cursor.next();
                                String priorityJson = document.toJson();
                                JSONObject priorityJsonObj = (JSONObject) JSON.parse(priorityJson);
                                prePriority = (int) priorityJsonObj.get("priority");
                            }
                            cursor.close();

                            FeedMessageRefer message = new FeedMessageRefer();
                            message.setId(duplicateId);
                            message.setPageLinkCount(pageLinkCount);
                            message.setTs(timeStamp);
                            if (messagePriority > prePriority) {
                                System.err.println("prePriority is:" + prePriority);
                                System.err.println("messagePriority is:" + messagePriority);
                                System.err.println("message hash is:" + strSH);
                                message.setSourcePriority(messagePriority);
                                db.getCollection("priority").updateOne(new Document("index", strSH),
                                        new Document("$set", new Document("priority", messagePriority)),
                                        new UpdateOptions().upsert(true));
                            }
                            queuedProducer.sendMessage("pageLinkCategories", message);

                            // hanxw-20160301: send duplicated message to a specific queue for async full-message storage
                            persistDuplicatedMessages(element, crawlId, duplicateId);

                            // hanxw-20160226: send to corresponding queue for comments processing if there's comment info in feedMessage
                            processComments(element, crawlId, duplicateId);
                        }
                    }
                } catch (Exception e) {
                    System.out.println("mongo Exception:" + e);
                    System.out.println("mongo Exception:" + e.getMessage());
                    e.printStackTrace();
                    for (StackTraceElement elem : e.getStackTrace()) {
                        System.out.println(elem);
                    }
                }
            }

            private void persistDuplicatedMessages(FeedMessage message, String crawlId, String duplicateId) {
                Map<String, Object> sourceMeta = message.getSourceMeta();
                if (null == sourceMeta) {
                    sourceMeta = new HashMap<String, Object>();
                    message.setSourceMeta(sourceMeta);
                }

                String messageUniqId = duplicateId + "-";
                if (!StringUtils.isBlank(crawlId)) {
                    messageUniqId += crawlId;
                } else {
                    messageUniqId += System.currentTimeMillis();
                }

                message.setId(messageUniqId);
                sourceMeta.put("duplicatedTo", duplicateId);

                String kafkaKey = GetMD5Code(message.getId());
                queuedProducer.sendMessage("duplicated-feed", kafkaKey, message);
            }

            private void processComments(FeedMessage message, String crawlId, String duplicateId) {
                Map<String, Object> extra = message.getExtra();
                if (null != extra && null != extra.get(FeedConstants.FEED_EXTRA_COMMENTS) && null != extra.get(FeedMessage.FEED_EXTRA_DEBUG_INFO) &&
                        null != ((Map) extra.get(FeedMessage.FEED_EXTRA_DEBUG_INFO)).get(FeedConstants.FEED_EXTRA_DEBUG_INFO_SOURCEID)) {
                    int sourceId = 0;
                    try {
                        sourceId = (int) ((Map) extra.get(FeedMessage.FEED_EXTRA_DEBUG_INFO)).get(FeedConstants.FEED_EXTRA_DEBUG_INFO_SOURCEID);
                    } catch (Exception exp) {
                        System.out.println(crawlId + ", warning: failed to extract source id information");
                    }

                    System.out.println(crawlId + ", Attention: this message(" + message.getId() + ") is duplicated to pre-processed news " + duplicateId + ", " +
                            "but found comments field in this message extra body, trying to process its comments. message info:[sourceId=" +
                            + sourceId + ", srcLink=" + message.getSrcLink() + "]");

                    try {

                        Map<String, Object> comments = (Map<String, Object>) extra.get(FeedConstants.FEED_EXTRA_COMMENTS);
                        List<String> commentLinks = (List<String>) comments.get(FeedConstants.FEED_EXTRA_COMMENTS_LINKS);
                        // sending links to comment crawler
                        if (null != commentLinks && commentLinks.size() > 0) {
                            CrawlRequest crawlRequest = new CrawlRequest();
                            crawlRequest.setStartingUrls(commentLinks);
                            crawlRequest.setRequestId(message.getId());
                            crawlRequest.setSourceId(sourceId);
                            crawlRequest.setAllowDuplication(true);

                            Map<String, Object> crawlRequestExtras = new HashMap<>();
                            crawlRequestExtras.put("messageType", extra.get(FeedConstants.FEED_SOURCE_META_DISPLAY_TEMPLATE));
                            crawlRequestExtras.put("messageId", duplicateId);  // merge comments for same news message
                            crawlRequestExtras.put("pubtime", message.getPubtime());
                            crawlRequest.setExtras(crawlRequestExtras);

                            System.out.println(crawlId + "(" + message.getId() + ")" + ", sending comment crawl request to comment crawler");
                            String kafkaKey = GetMD5Code(message.getId());
                            queuedProducer.sendMessage("crawl-request", kafkaKey, crawlRequest);
                        }

                        /**
                         * TODO: save items directly attached with news feed
                         *
                         * List<Map> commentItems = (List<Map>) comments.get(FeedConstants.FEED_EXTRA_COMMENTS_ITEMS);
                         */

                    } catch (Exception e) {
                        System.err.println(crawlId + ", exception to send comment crawl request to crawler, err:" + e.getMessage());
                    }
                }
            }

            @Override
            public void destroy() {

            }
        });

        consumerContainer.setMsgProcessorMap(msgProcessors);
        consumerContainer.start();

        for (String cid : cids) {
            try {
                String directory = "/data/Reduced/" + cid;
                File file = new File(directory);
                if (!file.isDirectory()) {
                    System.out.println("文件");
                    System.out.println("path=" + file.getPath());
                    System.out.println("absolutepath=" + file.getAbsolutePath());
                    System.out.println("name=" + file.getName());

                } else if (file.isDirectory()) {
                    File[] fileList = file.listFiles();
                    for (File f : fileList) {
                        if (f.isFile() && f.getName().endsWith(".txt")) {
                            //System.out.println("absolutepath = " + f.getAbsolutePath());
                            numArticles++;
                            NewsSimHash ns = new NewsSimHash(stopWords, readFile(f.getAbsolutePath()), false);
                            Map<String, Integer> wordsMap = ns.getWordsMap();

                            Integer value = null;
                            if ((value = numArticlesOfClass.get(cid)) != null) {
                                numArticlesOfClass.put(cid, value + 1);
                            } else {
                                numArticlesOfClass.put(cid, 1);
                            }

                            for (String word : wordsMap.keySet()) {
                                if ((value = numArticlesOfWord.get(word)) != null) {
                                    numArticlesOfWord.put(word, value + 1);
                                } else {
                                    numArticlesOfWord.put(word, 1);
                                }

                                Map<String, Integer> numArticlesWithWord = null;
                                numArticlesWithWord = numWordsInClass.get(cid);
                                if (numArticlesWithWord != null) {
                                    value = numArticlesWithWord.get(word);
                                    if (value != null) {
                                        numArticlesWithWord.put(word, value + 1);
                                    } else {
                                        numArticlesWithWord.put(word, 1);
                                    }
                                } else {
                                    numArticlesWithWord = new HashMap<String, Integer>();
                                    numArticlesWithWord.put(word, 1);
                                }
                                numWordsInClass.put(cid, numArticlesWithWord);
                            }
                        }
                    }
                }
            } catch (Exception e) {
                System.out.println("readfile()   Exception:" + e.getMessage());
                e.printStackTrace();
            }
        }


        System.out.println("the number of all the files: " + numArticles);

        System.out.println("the number of the files in classes: ");
        for (String classId : numArticlesOfClass.keySet()) {
            System.out.println("cid is: " + classId);
            System.out.println("The number of files is: " + numArticlesOfClass.get(classId));
        }

        System.out.println("the number of the files which contain one certain word: ");
        for (String word : numArticlesOfWord.keySet()) {
            System.out.println("word is: " + word);
            System.out.println("The number of files is: " + numArticlesOfWord.get(word));
        }

        System.out.println("the number of the files in a class which contain one certain word: ");
        for (String cid : numWordsInClass.keySet()) {
            System.out.println("class is: " + cid);
            Map<String, Integer> numArticlesWithWord = numWordsInClass.get(cid);
            for (String word : numArticlesWithWord.keySet()) {
                System.out.println("word is: " + word);
                System.out.println("The number of files is: " + numArticlesWithWord.get(word));
            }
        }

        /*
        Matrix matrix = new DenseMatrix(2, 2, new double[]{43,9,44,4});
        ChiSqTestResult independenceTestResult = Statistics.chiSqTest(matrix);
        System.out.println(independenceTestResult.statistic());
        */

        Map<String, Map<String, Double>> featureWordsOfClass = new HashMap<String, Map<String, Double>>();
        Set<String> featureWords = new HashSet<String>();
        for (String cid : cids) {
            Map<String, Double> result = new HashMap<String, Double>();
            Map<String, Integer> numArticlesWithWord = numWordsInClass.get(cid);
            for (String word : numArticlesWithWord.keySet()) {
                double N11 = numArticlesWithWord.get(word);
                double N10 = numArticlesOfWord.get(word) - N11;
                double N01 = numArticlesOfClass.get(cid) - N11;
                double N00 = numArticles - N11 - N10 - N01;

                // filter low-frequency words
                if (N11 * 50 < N11 + N01) {
                    continue;
                }

                Matrix matrix = new DenseMatrix(2, 2, new double[]{N11, N01, N10, N00});
                ChiSqTestResult independenceTestResult = Statistics.chiSqTest(matrix);
               /*
               System.out.println(word);
               System.out.println("N11: " + N11);
               System.out.println("N10: " + N10);
               System.out.println("N01: " + N01);
               System.out.println("N00: " + N00);
               */
                System.out.println(independenceTestResult.statistic());
                result.put(word, independenceTestResult.statistic());
            }

            featureWordsOfClass.put(cid, result);
        }

        for (String cid : featureWordsOfClass.keySet()) {
            System.out.println("cid is: " + cid);
            Map<String, Double> result = featureWordsOfClass.get(cid);

            List<Map.Entry<String, Double>> infoIds =
                    new ArrayList<Map.Entry<String, Double>>(result.entrySet());

            Collections.sort(infoIds, new Comparator<Map.Entry<String, Double>>() {
                public int compare(Map.Entry<String, Double> o1, Map.Entry<String, Double> o2) {
                    return (o2.getValue()).compareTo(o1.getValue());
                }
            });

            for (int i = 0; i < 200; i++) {
                String id = infoIds.get(i).toString();
                System.out.println(id);

                String word = infoIds.get(i).getKey();
                System.out.println(numWordsInClass.get(cid).get(word));
                featureWords.add(word);
            }
        }

        // training
        Map<String, Map<String, Integer>> wordCount = new HashMap<String, Map<String, Integer>>();
        Map<String, Map<String, Double>> probs1 = new HashMap<String, Map<String, Double>>();
        int numWords = 0;
        for (String cid : cids) {
            try {
                String directory = "/data/Reduced/" + cid + "/";

                int i;

                for (i = 100; i < 1000; i++) {
                    String fileName = directory + i + ".txt";
                    File file = new File(fileName);

                    if (file.exists() && file.isFile()) {
                        NewsSimHash ns = new NewsSimHash(stopWords, readFile(file.getAbsolutePath()), false);
                        Map<String, Integer> wordsMap = ns.getWordsMap();

                        for (String word : wordsMap.keySet()) {
                            if (!featureWords.contains(word)) {
                                continue;
                            }

                            Map<String, Integer> numWordInClass = wordCount.get(word);
                            if (numWordInClass != null) {
                                Integer value = numWordInClass.get(cid);
                                if (value != null) {
                                    numWordInClass.put(cid, value + wordsMap.get(word));
                                } else {
                                    numWordInClass.put(cid, wordsMap.get(word));
                                }
                            } else {
                                numWordInClass = new HashMap<String, Integer>();
                                numWordInClass.put(cid, wordsMap.get(word));
                            }

                            numWords += wordsMap.get(word);
                            wordCount.put(word, numWordInClass);
                        }
                    }

                }

            } catch (Exception e) {
                System.out.println("readfile()   Exception:" + e.getMessage());
                e.printStackTrace();
            }
        }

        System.out.println("numWords is : " + numWords);

        for (String word : wordCount.keySet()) {
            System.out.println("word is: " + word);
            Map<String, Integer> numWordInClass = wordCount.get(word);

            int wordOccurs = 0;
            for (String cid : numWordInClass.keySet()) {
                System.out.println("cid is: " + cid);
                System.out.println("number of word in cid is: " + numWordInClass.get(cid));
                wordOccurs += numWordInClass.get(cid);
            }
            System.out.println("word occurences is: " + wordOccurs);

            for (String cid : cids) {
                Integer numWord = numWordInClass.get(cid);
                if (numWord == null) {
                    numWord = 0;
                }
                Double p = (1.0 + numWord) / (numWords + wordOccurs);
                Map<String, Double> probsOfWord = probs1.get(word);
                if (probsOfWord == null) {
                    probsOfWord = new HashMap<String, Double>();
                }
                probsOfWord.put(cid, p);
                probs1.put(word, probsOfWord);
            }
        }

        for (String word : probs1.keySet()) {
            System.out.println("word is: " + word);
            Map<String, Double> prob = probs1.get(word);

            for (String cid : prob.keySet()) {
                System.out.println("cid is: " + cid);
                System.out.println("probability of word in cid is: " + prob.get(cid));
            }
        }

        int numTrue = 0;
        int numFalse = 0;
        for (String cid : cids) {
            try {
                String directory = "/data/Reduced/" + cid + "/";

                int i;

                for (i = 1000; i < 2000; i++) {
                    String fileName = directory + i + ".txt";
                    File file = new File(fileName);

                    if (file.exists() && file.isFile()) {
                        NewsSimHash ns = new NewsSimHash(stopWords, readFile(file.getAbsolutePath()), false);
                        Map<String, Integer> wordsMap = ns.getWordsMap();

                        String cidResult = null;
                        Double p_cidMax = null;
                        for (String _cid : cids) {
                            Double p_cid = Math.log(1.0 * numArticlesOfClass.get(_cid) / numArticles);
                            for (String word : wordsMap.keySet()) {
                                if (!probs1.containsKey(word)) {
                                    continue;
                                }


                                Map<String, Double> probOfClass = probs1.get(word);
                                Double p = probOfClass.get(_cid);
                                p_cid += Math.log(p) * wordsMap.get(word);
                            }
                            if (p_cidMax == null) {
                                p_cidMax = p_cid;
                                cidResult = _cid;
                            } else {
                                if (p_cid > p_cidMax) {
                                    p_cidMax = p_cid;
                                    cidResult = _cid;
                                }
                            }
                        }

                        if (cidResult.equals(cid)) {
                            numTrue++;
                        } else {
                            numFalse++;
                            System.out.println("==========");
                            System.out.println(file.getAbsolutePath());
                            System.out.println(cidResult);
                        }
                    }

                }

            } catch (Exception e) {
                System.out.println("readfile()   Exception:" + e.getMessage());
                e.printStackTrace();
            }
        }

        System.out.println("numTrue is: " + numTrue);
        System.out.println("numFalse is: " + numFalse);


        String str1 = null;
        String str2 = null;
        String str3 = null;
        String sentence = null;

        /*
        MongoClient mongoClient1 = new MongoClient(Arrays.asList(new ServerAddress(SimhashDB_IPAddress)));
        final MongoDatabase db1 = mongoClient1.getDatabase("news");
        MongoCollection newsContent = db1.getCollection("newsContent");

        //FindIterable iterable = newsContent.find(new Document("_id", "2000839ca9eebf15"));
        //MongoCursor cursor = iterable.iterator();
        while (cursor.hasNext()) {
            Document document = (Document)cursor.next();

            //System.out.println(document);
            //System.out.println(document.toJson());

            String newsStr = document.toJson();
            //System.out.println("**************** newsStr is: " + newsStr);
            JSONObject newsObj = (JSONObject)JSON.parse(newsStr);
            String title = (String)newsObj.get("title");
            String content = (String)newsObj.get("content");
            str1 = title + " " + content;
            str1 = HtmlRegexpUtil.filterHtml(str1);
            str1 = HtmlRegexpUtil.filterParentheses(str1);
            sentence = getLongestSentence(str1);
            str1 = str1 + sentence;
            System.out.println("**************** str1 is: " + str1);
            System.out.println("**************** the length of str1 is: " + str1.length());

            str1 = "足协公布国奥队集训名单:恒大4人 3留洋小将入选";
            str1 = HtmlRegexpUtil.filterParentheses(str1);
            str1 = HtmlRegexpUtil.filterTitleChars(str1);
            //
            KeyWordComputer key = new KeyWordComputer(10);
            Iterator it = key.computeArticleTfidf(str1).iterator();
            while(it.hasNext()) {
                Keyword key2 = (Keyword)it.next();
                System.out.println(key2.toString());
            }
            //
        }
        cursor.close();

        iterable = newsContent.find(new Document("_id", "20180295cab66bf8"));
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
            str2 = title + " " + content;
            str2 = HtmlRegexpUtil.filterHtml(str2);
            str2 = HtmlRegexpUtil.filterParentheses(str2);
            sentence = getLongestSentence(str2);
            str2 = str2 + sentence;
            System.out.println("**************** str2 is: " + str2);
            System.out.println("**************** the length of str2 is: " + str2.length());

            str2 = "足协公布国奥队集训名单:恒大四人三留洋小将入选（图）(图)";
            str2 = HtmlRegexpUtil.filterParentheses(str2);
            str2 = HtmlRegexpUtil.filterTitleChars(str2);
            KeyWordComputer key = new KeyWordComputer(10);
            Iterator it = key.computeArticleTfidf(str2).iterator() ;
            while(it.hasNext()) {
                Keyword key2 = (Keyword)it.next();
                System.out.println(key2.toString());
            }
    }

    cursor.close();

    iterable=newsContent.find(new

    Document("_id","2000838fca7ce138")

    );
    cursor=iterable.iterator();
    while(cursor.hasNext())

    {
        Document document = (Document) cursor.next();

        //System.out.println(document);
        //System.out.println(document.toJson());

        String newsStr = document.toJson();
        //System.out.println("**************** newsStr is: " + newsStr);
        JSONObject newsObj = (JSONObject) JSON.parse(newsStr);
        String title = (String) newsObj.get("title");
        String content = (String) newsObj.get("content");
        str3 = title + " " + content;
        str3 = HtmlRegexpUtil.filterHtml(str3);
        str3 = HtmlRegexpUtil.filterParentheses(str3);
        sentence = getLongestSentence(str3);
        str3 = str3 + sentence;
        System.out.println("**************** str3 is: " + str3);
        System.out.println("**************** the length of str3 is: " + str3.length());
    }

    cursor.close();


    NewsSimHash ns0 = new NewsSimHash(stopWords, str1, true);
    NewsSimHash ns1 = new NewsSimHash(stopWords, str2, true);
    NewsSimHash ns2 = new NewsSimHash(stopWords, str3, true);

    //simhash的二进制表示
    System.out.println("simhash的二进制表示:");
    System.out.println(ns0.getStrSimHash());
    System.out.println(ns1.getStrSimHash());
    System.out.println(ns2.getStrSimHash());
    //simhash的BigInteger表示
    System.out.println("simhash的BigInteger表示:");
    System.out.println(ns0.getIntSimHash().

    toString(16)

    );
    System.out.println(ns1.getIntSimHash().

    toString(16)

    );
    System.out.println(ns2.getIntSimHash().

    toString(16)

    );
    //ns1与ns的汉明距离,
    System.out.println("ns1与ns0的汉明距离");
    System.out.println(ns0.hammingDistance(ns1));
    System.out.println("ns2与ns0的汉明距离");
    System.out.println(ns0.hammingDistance(ns2));
    System.out.println("ns1与ns2的汉明距离");
    System.out.println(ns1.hammingDistance(ns2));
    */

        for (; ; )

        {
            try {
                TimeUnit.SECONDS.sleep(80000);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        //consumerContainer.stop();
    }

    public static String toHexString(String s) {
        String str = "";
        for (int i = 0; i < s.length(); i++) {
            int ch = (int) s.charAt(i);
            String s4 = Integer.toHexString(ch);
            str = str + s4;
        }
        return str;
    }

    public static boolean isControlChar(String str) {
        String s = new String(str.getBytes());
        String pattern = "[\u0000-\u00ff]+";
        Pattern p = Pattern.compile(pattern);
        Matcher result = p.matcher(s);
        return result.matches(); //是否不含有中文字符
    }

    public static String getLongestSentence(String str) {
        String result = "";

        if (str == null) {
            return result;
        }
        str = HtmlRegexpUtil.filterSpecialChars(str);
        String separator = "[\\s\uff0c\u3002\u3000,]";
        String[] strArr = str.split(separator);

        if (strArr == null) {
            return result;
        }


        /*
        Map<String, Integer> sentences = new HashMap<String, Integer>();
        for (String s : strArr) {
            sentences.put(s, s.length());
        }

        List<Map.Entry<String, Integer>> sentenceLengths =
        new ArrayList<Map.Entry<String, Integer>>(sentences.entrySet());

        Collections.sort(sentenceLengths, new Comparator<Map.Entry<String, Integer>>() {
            public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                return (o2.getValue()).compareTo(o1.getValue());
            }
        });

        int nSentences = 1;
        if (sentenceLengths.size() < 1) {
            nSentences = sentenceLengths.size();
        }
        */

        int max = 0;
        String sentence = "";
        for (String s : strArr) {
            if (isControlChar(s)) {
                continue;
            }
            int length = 0;
            for (int i = 0; i < s.length(); i++) {
                char ch = s.charAt(i);
                if ((ch > 0) && (ch < 0xff)) {
                    continue;
                }
                length++;
            }

            if (length > max) {
                max = length;
                sentence = s;
            }
        }

        /*
        for (int i = 0; i < nSentences; i++) {
            String sentence = sentenceLengths.get(i).getKey();
            //int j = nSentences - i;
            int j = 5;
            for (int k = 0; k < j; k++) {
                result = result + " " + sentence;
            }
        }
        */
        int j = 5;
        for (int k = 0; k < j; k++) {
            result = result + " " + sentence;
        }

        //System.out.println("the longest sentence is: " + result);
        return result;
    }

    public static List<String> getLongestSentences(String str) {
        List<String> result = new ArrayList<String>();

        String[] skipSentences = new String[]{"邀请好友立得50元跟投基金",
                "亲，如果你喜欢小编发的衣服可以加小编微信哟",
                "攻略秘册，边玩游戏边用手机看攻略",
                "如果你不会穿衣打扮？建议你关注我们",
                "笑奇葩，不信你试试！下看片神器",
                "想要获取更多有意思的内容，请移步界面网站首页",
                "本文系哒哒进口独家编译，任何媒体或网站未经书面授权不得转载",
                "▼▼▼▼还不够？更多劲爆内容，欢迎关注我们的微信公众号",
                "哈哈,那就请加我们这里应有尽有",
                "xhsbcom，学一点健康养生知识",
                "咨询，可在线互动，关注微信公众号",
                "以上图文均转自互联网",
                "新浪网登载此文",
                "若无特别注明",
                "yulefan8",
                "妹纸搞笑视频",
                "关注我们的微信公众号",
                "搞笑我们是认真的",
                "手机人民网登载此文",
                "本文来源前瞻网",
                "腾讯科技精选优质自媒体文章",
                "我们每天会在微信上推送",
                "午夜时分，他都会带你来到这里",
                "严肃八卦禁止任何微信号",
                "「深夜俱乐部」是数字尾巴开设的栏目",
                "本漫画整理自网络"
        };

        if (str == null) {
            return result;
        }

        str = filterSpecialOrigin(str);
        str = filterSpecialOrigin1(str);
        //str = filterSpecialOrigin2(str);
        str = filterSpecialOrigin3(str);
        str = filterSpecialChars(str);
        String separator = ":|\\?|!|\\s+|\u3002|\u3000+|\u3001|\uFF1A|\uFF1F|《|！| ";
        String[] strArr = str.split(separator);

        if (strArr == null) {
            return result;
        }

        String sentence = "";
        int number = 0;
        for (String s : strArr) {
            boolean skip = false;
            if (isControlChar(s)) {
                continue;
            }

            for (String skipSentence : skipSentences) {
                if (s.startsWith(skipSentence)) {
                    skip = true;
                    break;
                }
            }
            if (skip) {
                continue;
            }

            int length = 0;
            for (int i = 0; i < s.length(); i++) {
                char ch = s.charAt(i);
                if ((ch > 0) && (ch < 0xff)) {
                    continue;
                }
                length++;
            }

            if (length > 26) {
                result.add(s);
                number++;
                if (number == 3) {
                    break;
                }
            }
        }

        return result;
    }

    // 返回形式为数字跟字符串
    private static String byteToArrayString(byte bByte) {
        int iRet = bByte;
        // System.out.println("iRet="+iRet);
        if (iRet < 0) {
            iRet += 256;
        }
        int iD1 = iRet / 16;
        int iD2 = iRet % 16;
        return strDigits[iD1] + strDigits[iD2];
    }

    // 返回形式只为数字
    private static String byteToNum(byte bByte) {
        int iRet = bByte;
        System.out.println("iRet1=" + iRet);
        if (iRet < 0) {
            iRet += 256;
        }
        return String.valueOf(iRet);
    }

    // 转换字节数组为16进制字串
    private static String byteToString(byte[] bByte) {
        StringBuffer sBuffer = new StringBuffer();
        for (int i = 0; i < bByte.length; i++) {
            sBuffer.append(byteToArrayString(bByte[i]));
        }
        return sBuffer.toString();
    }

    public static String GetMD5Code(String strObj) {
        String resultString = null;
        try {
            resultString = new String(strObj);
            MessageDigest md = MessageDigest.getInstance("MD5");
            // md.digest() 该函数返回值为存放哈希值结果的byte数组
            resultString = byteToString(md.digest(strObj.getBytes()));
        } catch (NoSuchAlgorithmException ex) {
            ex.printStackTrace();
        }
        return resultString;
    }

    public static void addSensitiveWordToHashMap(Set<String> keyWordSet, Map sensitiveWordMap) {
        String key = null;
        Map nowMap = null;
        Map<String, String> newWorMap = null;

        Iterator<String> iterator = keyWordSet.iterator();
        while (iterator.hasNext()) {
            key = iterator.next();
            nowMap = sensitiveWordMap;
            for (int i = 0; i < key.length(); i++) {
                char keyChar = key.charAt(i);
                Object wordMap = nowMap.get(keyChar);

                if (wordMap != null) {
                    nowMap = (Map) wordMap;
                } else {
                    newWorMap = new HashMap<String, String>();
                    newWorMap.put("isEnd", "0");
                    nowMap.put(keyChar, newWorMap);
                    nowMap = newWorMap;
                }

                if (i == key.length() - 1) {
                    nowMap.put("isEnd", "1");
                }
            }
        }
    }

    public static int CheckSensitiveWord(String txt, int beginIndex, Map sensitiveWordMap) {
        boolean flag = false;
        int matchFlag = 0;
        char word = 0;
        Map nowMap = sensitiveWordMap;
        for (int i = beginIndex; i < txt.length(); i++) {
            word = txt.charAt(i);
            nowMap = (Map) nowMap.get(word);
            if (nowMap != null) {
                matchFlag++;
                if ("1".equals(nowMap.get("isEnd"))) {
                    flag = true;
                    break;
                }
            } else {
                break;
            }
        }
        if (!flag) {
            matchFlag = 0;
        }
        return matchFlag;
    }

    public static List<String> getTxtKeyWords(String txt, Map sensitiveWordMap) {
        List<String> list = new ArrayList<String>();
        int length = txt.length();
        for (int i = 0; i < length; ) {
            int len = CheckSensitiveWord(txt, i, sensitiveWordMap);
            if (len > 0) {
                String tt = txt.substring(i, i + len);
                list.add(tt);
                i += len;
            } else {
                i++;
            }
        }
        return list;
    }

    public static String filterSpecialChars(String str) {
        Pattern pattern = Pattern.compile("\\]|#|\\[|】|【|\\d+");
        Matcher matcher = pattern.matcher(str);
        StringBuffer sb = new StringBuffer();
        boolean result1 = matcher.find();
        while (result1) {
            matcher.appendReplacement(sb, " ");
            result1 = matcher.find();
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    public static String filterSpecialOrigin(String str) {
        Pattern pattern = Pattern.compile(" \\n ");
        Matcher matcher = pattern.matcher(str);
        StringBuffer sb = new StringBuffer();
        boolean result1 = matcher.find();
        while (result1) {
            matcher.appendReplacement(sb, "");
            result1 = matcher.find();
        }
        matcher.appendTail(sb);
        return sb.toString();
    }


    public static String filterSpecialOrigin1(String str) {
        Pattern pattern = Pattern.compile("，\\s");
        Matcher matcher = pattern.matcher(str);
        StringBuffer sb = new StringBuffer();
        boolean result1 = matcher.find();
        while (result1) {
            matcher.appendReplacement(sb, "，");
            result1 = matcher.find();
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    public static String filterSpecialOrigin2(String str) {
        Matcher matcher = origin2Pattern.matcher(str);
        StringBuffer sb = new StringBuffer();
        boolean result1 = matcher.find();
        while (result1) {
            String replacement = "";
            if (matcher.group(1) != null) {
                replacement = matcher.group(1);
            } else if (matcher.group(2) != null) {
                replacement = matcher.group(2);
            } else if (matcher.group(3) != null) {
                replacement = matcher.group(3);
            } else if (matcher.group(4) != null) {
                replacement = matcher.group(4);
            } else if (matcher.group(5) != null) {
                replacement = matcher.group(5);
            } else if (matcher.group(6) != null) {
                replacement = matcher.group(6);
            }
            matcher.appendReplacement(sb, replacement);
            result1 = matcher.find();
        }
        matcher.appendTail(sb);
        return sb.toString();
    }


    public static String filterSpecialOrigin3(String str) {
        Pattern pattern = Pattern.compile("\\s+([a-zA-Z]+)\\s+");
        Matcher matcher = pattern.matcher(str);
        StringBuffer sb = new StringBuffer();
        boolean result1 = matcher.find();
        while (result1) {
            matcher.appendReplacement(sb, matcher.group(1));
            result1 = matcher.find();
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    public static String processContent(String content) {
        String processedContent;

        processedContent = HtmlRegexpUtil.filterA(content);
        processedContent = HtmlRegexpUtil.filterHtml(processedContent);
        processedContent = HtmlRegexpUtil.filterParentheses(processedContent);
        processedContent = filterSpecialOrigin2(processedContent);

        return processedContent;
    }
}
