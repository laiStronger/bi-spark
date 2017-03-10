/*
 * 文件名：HBaseFamilyAnnotation.java
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
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface HBaseFieldAnnotation {

    /**
     * 列族的family值
     * 
     * @return
     * @author tanguojun
     */
    public String familyName();


    /**
     * 字段名
     *
     * @return
     * @author tanguojun
     */
    public String columnName();

}
