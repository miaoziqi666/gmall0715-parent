package com.atguigu.gmall0715.publisher.service.impl;

import com.atguigu.gmall0715.common.constant.GmallConstant;
import com.atguigu.gmall0715.publisher.mapper.DauMapper;
import com.atguigu.gmall0715.publisher.mapper.OrderMapper;
import com.atguigu.gmall0715.publisher.service.PublisherService;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublisherServiceImpl implements PublisherService {

    @Autowired
    DauMapper dauMapper;

    @Autowired
    OrderMapper orderMapper;

    @Autowired
    JestClient jestClient;

    @Override
    public Long getDauCount(String date) {
        return dauMapper.selectDauCount(date);
    }

    @Override
    public Map getDauCountHour(String date) {

        Map resultMap=new HashMap();

        //结构转换
        //  [{"hour":15,"ct":428},{"hour":16,"ct":667}]
        List<Map> mapList = dauMapper.selectDauCountHour(date);
        //{"15":428,"16":667}
        for (Map map : mapList) {
            resultMap.put(  map.get("hour"),map.get("ct"));
        }

        return resultMap;
    }

    @Override
    public Double getOrderAmount(String date) {
        return orderMapper.selectOrderAmount(date);
    }

    @Override
    public Map getOrderAmountHour(String date) {

        List<Map> mapList = orderMapper.selectOrderAmountHour(date);
        Map resultMap=new HashMap();
        for (Map map : mapList) {
            resultMap.put(map.get("CREATE_HOUR"),map.get("ORDER_AMOUNT"));
        }

        return resultMap;
    }

    @Override
    public Map getSaleDeatil(String date, String keyword, int pageSize, int pageNo) {
        String query = "";
        Search search = new Search.Builder(query).addIndex(GmallConstant.KAFKA_TOPIC_EVENT).addType("_doc").build();
        try {
            SearchResult searcchResult = jestClient.execute(search);
            searcchResult.getAggregations();
//            searcchResult.getHits();
            searcchResult.getTotal();

        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;

    }


}
