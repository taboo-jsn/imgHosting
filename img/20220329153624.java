package com.taboo.vendor_data_access_down.handler.gadata.wechat;

import cn.hutool.core.codec.Base64;
import cn.hutool.db.nosql.redis.RedisDS;
import cn.hutool.setting.Setting;
import com.alibaba.fastjson.JSONObject;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ReadContext;
import com.taboo.vendor_data_access_down.entity.Task;
import com.taboo.vendor_data_access_down.util.CommonUtil;
import com.taboo.vendor_data_access_down.util.DateUtils;
import org.apache.commons.lang3.StringUtils;
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Copyright (C), 2021-2022, 慧科讯业
 * FileName: com.taboo.vendor_data_access_down.handler.gadata.wechat
 * Author:   hanmr
 * Date:     2022/1/10 6:23 下午
 * History:
 * <author>          <time>          <version>          <desc>
 * 作者姓名           修改时间           版本号              描述
 */
public class PostDown {
    private static final Logger log = LoggerFactory.getLogger( PostDown.class );

    public static boolean down(Setting setting, String env, Task task, Jedis jedis){
        boolean flag = false;
        try{
            String url = "http://projects-databus.gsdata.cn:7777/huike/huike/get-news";
            Map<String,String> params = new HashMap<>();
            params.put("project_id",setting.getByGroup("gsdata.wechat.project",env));
            params.put("sign",setting.getByGroup("gsdata.wechat.sign",env));
            params.put("media_id",task.getBean_id());
            params.put("start_time", DateUtils.customStart(0,-3,1));
            params.put("end_time", DateUtils.todayEnd());
            params.put("page","1");
            params.put("limit","20");
            Connection conn = Jsoup.connect(url).ignoreContentType(true).validateTLSCertificates(false).maxBodySize(30000000).timeout(30000000);
            conn.data(params);
            String content = conn.post().text();
            if(StringUtils.isNotBlank(content)){
                if(CommonUtil.judgeBody(content)){
                    ReadContext ctx = CommonUtil.getReadContext(content);
                    List<Integer> numFound = ctx.read("$.data.numFound");
                    int pageTotle = (numFound.get(0) - 1) / 20 + 1;
                    for(int page = 1 ;page <= pageTotle ; page++){
                        JSONObject json = new JSONObject();
                        if(page == 1) {
                            json.put("vendor",task.getVendor());
                            json.put("platfrom",task.getPlatfrom());
                            json.put("type","post");
                            json.put("doc",content);
                            jedis.lpush("gsdata_doc", Base64.encode(json.toJSONString()));
                        }else{
                            params.put("page", String.valueOf(page));
                            conn.data(params);
                            content = conn.post().text();
                            if(StringUtils.isNotBlank(content)){
                                if(CommonUtil.judgeBody(content)){
                                    json.put("vendor",task.getVendor());
                                    json.put("platfrom",task.getPlatfrom());
                                    json.put("type","post");
                                    json.put("doc",content);
                                    jedis.lpush("gsdata_doc", Base64.encode(json.toJSONString()));
                                }else
                                    log.error("id : {} post 响应报文非正常 : {}",task.getBean_id(),content);
                            }else
                                log.error("id : {} post 响应报文为空",task.getBean_id());
                        }
                    }
                    flag = true;
                }else
                    log.error("id : {} post 响应报文非正常 : {}",task.getBean_id(),content);
            }else
                log.error("id : {} post 响应报文为空",task.getBean_id());
        } catch(Exception e){
            flag = false;
            log.error("id : {} 获取 post 信息异常 : {}",task.getBean_id(),e.getMessage());
        }
        return flag;
    }
}
