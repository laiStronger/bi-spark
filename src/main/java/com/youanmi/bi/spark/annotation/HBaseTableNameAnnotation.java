/*
 * 文件名：HBaseTableNameAnnotation.java
 * 版权：深圳柚安米科技有限公司版权所有
 * 修改人：tanguojun
 * 修改时间：2016年11月30日
 * 修改内容：新增
 */
package com.youanmi.bi.spark.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


/**
 * hbase列family
 * 
 * @author tanguojun
 * @since 2.2.4
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface HBaseTableNameAnnotation {

    /**
     * 表名
     * 
     * @return
     * @author tanguojun
     */
    public String value();

}
