package com.xwz.retail.v1.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @Package com.xwz.retail.v1.realtime.bean.CartAddUuBean
 * @Author  Wenzhen.Xie
 * @Date  2025/4/8 13:51
 * @description:
 */
@Data
@AllArgsConstructor
public class CartAddUuBean {
    // 窗口起始时间
    String stt;
    // 窗口闭合时间
    String edt;
    // 当天日期
    String curDate;
    // 加购独立用户数
    Long cartAddUuCt;
}
