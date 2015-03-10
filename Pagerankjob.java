package org.conan.myhadoop.pagerank;

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.mapred.JobConf;

public class PageRankJob {

    public static final String HDFS = "hdfs://192.168.1.210:9000";
    public static final Pattern DELIMITER = Pattern.compile("[\t,]");

    public static void main(String[] args) {
        // Map<String, String> path = pagerank(); //pagerank dataset

        Map<String, String> path = peoplerank();// peoplerank dataset

        try {
            AdjacencyMatrix.run(path);
            int iter = 10;
            for (int i = 0; i < iter; i++) {// recursive
                PageRank.run(path);
            }
            Normal.run(path);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(0);
    }

    private static Map<String, String> peoplerank() {
        Map<String, String> path = new HashMap<String, String>();
        path.put("page", "logfile/pagerank/people.csv");// local data file
        path.put("pr", "logfile/pagerank/peoplerank.csv");// local data file
        path.put("nums", "25");// num of users
        path.put("d", "0.85");// damping coefficienncy

        path.put("input", HDFS + "/user/hdfs/pagerank");// HDFS directory
        path.put("input_pr", HDFS + "/user/hdfs/pagerank/pr");// pr directory
        path.put("tmp1", HDFS + "/user/hdfs/pagerank/tmp1");// temporary directory,store adjacency matrix
        path.put("tmp2", HDFS + "/user/hdfs/pagerank/tmp2");// tempory directory,calculate PR, recover input_pr

        path.put("result", HDFS + "/user/hdfs/pagerank/result");// calculate PR
        return path;

    }

    private static Map<String, String> pagerank() {
        Map<String, String> path = new HashMap<String, String>();
        path.put("page", "logfile/pagerank/page.csv");// local data file
        path.put("pr", "logfile/pagerank/pr.csv");// local data file
        path.put("nums", "4");// num of pages
        path.put("d", "0.85");// damping coefficiency

        path.put("input", HDFS + "/user/hdfs/pagerank");// HDFS file firectory
        path.put("input_pr", HDFS + "/user/hdfs/pagerank/pr");// pr store directory
        path.put("tmp1", HDFS + "/user/hdfs/pagerank/tmp1");// temporary file,store adjancy matrix
        path.put("tmp2", HDFS + "/user/hdfs/pagerank/tmp2");// tempory directory,calculate PR, recover input_pr

        path.put("result", HDFS + "/user/hdfs/pagerank/result");// calculate PR
        return path;
    }

    public static JobConf config() {// Hadoop cluster configuration
        JobConf conf = new JobConf(PageRankJob.class);
        conf.setJobName("PageRank");
        conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/hdfs-site.xml");
        conf.addResource("classpath:/hadoop/mapred-site.xml");
        return conf;
    }

    public static String scaleFloat(float f) {// data format as six digit floating number
        DecimalFormat df = new DecimalFormat("##0.000000");
        return df.format(f);
    }

}