package com.xwz.retail.v2.reaktime.bean;

import com.alibaba.fastjson.annotation.JSONField;

import java.math.BigDecimal;

/**
 * @Package com.xwz.retail.v2.reaktime.bean.DamopanBean
 * @Author  Wenzhen.Xie
 * @Date  2025/5/12 16:28
 * @description: 
*/

public class DamopanBean {
    // 窗口起始时间
    String stt;
    // 窗口结束时间
    String edt;
    // 当天日期
    String curDate;
    // 品牌 ID
    String trademarkId;
    // 品牌名称
    String trademarkName;
    // 一级品类 ID
    String category1Id;
    // 一级品类名称
    String category1Name;
    // sku_id
    String skuId;
    // sku 名称
    String skuName;
    // 下单金额
    BigDecimal orderAmount;
    //用户ID
    String userId;
    //用户名称
    String userName;
    //用户年龄
    String userAge;
    //用户性别
    String userGender;
    //用户年代
    String userEra;
    //用户身高
    String userHeight;
    //用户体重
    String userWeight;
    //用户星座
    String userConstellation;
    // 时间戳
    @JSONField(serialize = false)
    Long ts_ms;
}
