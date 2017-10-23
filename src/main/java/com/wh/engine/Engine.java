package com.wh.engine;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.wh.dao.MysqlHelper;
import com.wh.util.DateUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2017/10/18 0018.
 */
public class Engine {
    public static void main(String[] args) {
        Document doc = null;
        final String url = "http://pdfm2.eastmoney.com/EM_UBG_PDTI_Fast/api/js?id=CL00Y0&TYPE=r&js=fsdata((x))&rtntype=5&isCR=false&fsdata=fsdata";
        String insertSql = "insert into yuanyou values (?,?,?,?,?,?,?)";
        String querySql = "select * from yuanyou where time in(";
        String updateSql = "update yuanyou set val=?,val2=?,val3=?,val4=?,updatetime=? where time=?";
        MysqlHelper mysqlHelper = new MysqlHelper();
        List<String[]> updateParams = new ArrayList<String[]>();
        List<String[]> insertParams = new ArrayList<String[]>();
        List<String> timeList = new ArrayList<String>();
        Map<String, String[]> data = new HashMap<String, String[]>();

        try {
            System.out.println("------start-----");
            long startTime = System.currentTimeMillis();
            String createTime = DateUtils.now();
            doc = Jsoup.connect(url).get();
            String body = doc.body().text();
            body = body.substring(7, body.length() - 1);
            JSONObject jsonObject = JSON.parseObject(body);
            JSONArray array = JSON.parseArray(jsonObject.get("data").toString());
            System.out.println(array);
            for (Object obj : array) {
                String[] split = obj.toString().split(",");
                data.put(split[0], split);
                timeList.add(split[0]);
            }
            List<String> query = getQuerySql(timeList, querySql);
            for (String sql : query) {
                List<Map<String, Object>> res = mysqlHelper.query(sql, null);
                for (Map<String, Object> map : res) {
                    String time = map.get("time").toString();
                    String[] split = data.get(time);
                    updateParams.add(new String[]{split[1], split[2], split[3], split[4], createTime,split[0]});
                    data.remove(time);
                }
            }
            for (String key : data.keySet()) {
                String[] split = data.get(key);
                insertParams.add(new String[]{split[0], split[1], split[2], split[3], split[4], createTime,createTime});
            }
//            System.out.println(updateParams.size());
//            System.out.println(insertParams.size());
            if (updateParams.size() > 0) {
                mysqlHelper.executeBatch(updateSql, updateParams);
            }
            if (insertParams.size() > 0) {
                mysqlHelper.executeBatch(insertSql, insertParams);
            }

            long endTime =System.currentTimeMillis();
            System.out.println("cost "+(endTime-startTime)/1000 +" s");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            mysqlHelper.close();
        }
    }

    private static List<String> getQuerySql(List<String> timeList, String querySql) {
        List<String> list = new ArrayList<String>();
        StringBuffer time = new StringBuffer();
        String query = "";
        for (int i = 0; i < timeList.size(); i++) {
            time.append("'" + timeList.get(i) + "'");
            if (i % 200 != 0 || i == 0) {
                time.append(",");

            } else {
                time.append(")");
                query = querySql + time.toString();
//                System.out.println(query);
                time.setLength(0);
                list.add(query);
            }
        }
        query = querySql + time.toString().substring(0, time.length() - 1) + ")";
//        System.out.println(query);
        list.add(query);
        return list;
    }

}
