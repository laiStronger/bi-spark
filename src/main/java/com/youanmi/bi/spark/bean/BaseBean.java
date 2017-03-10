/*
 * 文件名：BaseBean.java
 * 版权：深圳柚安米科技有限公司版权所有
 * 修改人：tanguojun
 * 修改时间：2016年11月30日
 * 修改内容：新增
 */
package com.youanmi.bi.spark.bean;

import java.io.Serializable;


/**
 * hbase base bean
 * 
 * @author tanguojun
 * @since 2.2.4
 */
public class BaseBean implements Serializable {

    /**
     * rowkey
     */
    protected String rowKey;


    public String getRowKey() {
        return rowKey;
    }


    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

}
