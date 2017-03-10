/*
 * 文件名：UserRegister.java
 * 版权：深圳柚安米科技有限公司版权所有
 * 修改人：tanguojun
 * 修改时间：2016年11月30日
 * 修改内容：新增
 */
package com.youanmi.bi.spark.bean;

import java.io.Serializable;

import com.youanmi.bi.spark.annotation.HBaseFieldAnnotation;
import com.youanmi.bi.spark.annotation.HBaseTableNameAnnotation;


/**
 * 用户注册
 * 
 * @author tanguojun
 * @since 2.2.4
 */
@HBaseTableNameAnnotation(User.TABLE_NAME)
public class User extends BaseBean implements Serializable {

    /**
     * hbase表名
     */
    public static final String TABLE_NAME = "account:user";

    public interface Familys {

        /**
         * address-family
         */
        String ADDRESS_FAMILY = "address";
    }

    /**
     * hbase 存储字段名称
     * 
     * @author tanguojun
     *
     */
    public interface Columns {

        String PROVINCE_ID = "provinceId";

        String PROVINCE = "province";

        String SEX = "sex";

        String TIME = "time";

    }

    @HBaseFieldAnnotation(familyName = Familys.ADDRESS_FAMILY, columnName = Columns.PROVINCE_ID)
    private Integer provinceId;

    @HBaseFieldAnnotation(familyName = Familys.ADDRESS_FAMILY, columnName = Columns.PROVINCE)
    private String province;

    @HBaseFieldAnnotation(familyName = Familys.ADDRESS_FAMILY, columnName = Columns.SEX)
    private String sex;

    @HBaseFieldAnnotation(familyName = Familys.ADDRESS_FAMILY, columnName = Columns.TIME)
    private Long time;


    public User(String rowKey, Integer provinceId, String province, String sex, Long time) {
        super();
        this.rowKey = rowKey;
        this.provinceId = provinceId;
        this.province = province;
        this.sex = sex;
        this.time = time;
    }


    public User() {
        super();
    }


    public String getRowKey() {
        return rowKey;
    }


    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }


    public Integer getProvinceId() {
        return provinceId;
    }


    public void setProvinceId(Integer provinceId) {
        this.provinceId = provinceId;
    }


    public String getProvince() {
        return province;
    }


    public void setProvince(String province) {
        this.province = province;
    }


    public String getSex() {
        return sex;
    }


    public void setSex(String sex) {
        this.sex = sex;
    }


    public Long getTime() {
        return time;
    }


    public void setTime(Long time) {
        this.time = time;
    }


    @Override
    public String toString() {
        return "UserRegister [provinceId=" + provinceId + ", province=" + province + ", sex=" + sex
                + ", time=" + time + ", rowKey=" + rowKey + "]";
    }

}
