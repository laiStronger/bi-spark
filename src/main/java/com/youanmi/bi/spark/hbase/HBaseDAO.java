/*
 * 文件名：HBaseDAO.java
 * 版权：深圳柚安米科技有限公司版权所有
 * 修改人：tanguojun
 * 修改时间：2016年11月30日
 * 修改内容：新增
 */
package com.youanmi.bi.spark.hbase;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.youanmi.bi.kafkaSpark.demo.KafkaProducerDemo;
import com.youanmi.bi.spark.annotation.HBaseFieldAnnotation;
import com.youanmi.bi.spark.annotation.HBaseTableNameAnnotation;
import com.youanmi.bi.spark.bean.BaseBean;
import com.youanmi.bi.spark.bean.User;


/**
 * hbase-dao
 * 
 * @author tanguojun
 * @since 2.2.4
 */
public class HBaseDAO {

    public static final String ROWKEY = "rowKey";

    /**
     * 调测日志记录器。
     */
    private static final Logger LOG = LoggerFactory.getLogger(HBaseDAO.class);

    private static Connection connection;


    /**
     * 获取hbase连接,单例
     * 
     * @return
     * @throws IOException
     */
    public static Connection getInstance() throws IOException {

        // 判断连接是否为空或者关闭
        if (connection == null || connection.isClosed()) {

            LOG.info("create hbase connection.");

            Configuration configuration = HBaseConfiguration.create();
            // 设置zk服务器ip,多个zk用逗号分隔
            configuration.set("hbase.zookeeper.quorum", "192.168.1.30");
            // 设置zk端口号
            configuration.set("hbase.zookeeper.property.clientPort", "2181");

            // 创建连接
            connection = ConnectionFactory.createConnection(configuration);
        }

        return connection;
    }


    /**
     * 扫描表
     *
     * @param beanClass
     * @return
     * @throws IOException
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws NumberFormatException
     * @throws IllegalArgumentException
     * @throws InvocationTargetException
     * @throws IntrospectionException
     * @author tanguojun
     */
    public static <T extends BaseBean> List<T> scan(Class<T> beanClass) throws IOException,
            InstantiationException, IllegalAccessException, NumberFormatException, IllegalArgumentException,
            InvocationTargetException, IntrospectionException {

        // 获取bean上的注解,获取表名
        String tableName =
                ((HBaseTableNameAnnotation) beanClass.getAnnotation(HBaseTableNameAnnotation.class)).value();

        // 获取表
        Table table = getInstance().getTable(TableName.valueOf(tableName));

        Scan scan = new Scan();

        ResultScanner resultScanner = table.getScanner(scan);

        List<T> list = new ArrayList<T>();

        if (resultScanner != null) {

            // 获取class定义的所有属性
            Field[] fields = beanClass.getDeclaredFields();

            Map<String, Field> fieldMap = new HashMap<String, Field>();

            // 遍历属性拿到注解
            for (Field field : fields) {

                HBaseFieldAnnotation annotation = field.getAnnotation(HBaseFieldAnnotation.class);

                if (annotation != null) {
                    fieldMap.put(annotation.familyName() + ":" + annotation.columnName(), field);
                }
            }

            for (Result result : resultScanner) {

                BaseBean bean = beanClass.newInstance();

                bean.setRowKey(new String(result.getRow(), "UTF-8"));

                for (Cell cell : result.rawCells()) {

                    // 获取family
                    String family =
                            new String(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(),
                                "UTF-8");
                    // 获取column
                    String qualifier =
                            new String(cell.getQualifierArray(), cell.getQualifierOffset(),
                                cell.getQualifierLength(), "UTF-8");
                    // 获取值
                    String value =
                            new String(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength(),
                                "UTF-8");

                    // 获取属性
                    Field field = fieldMap.get(family + ":" + qualifier);

                    if (field != null) {
                        PropertyDescriptor pd = new PropertyDescriptor(field.getName(), beanClass);
                        // 获得写方法
                        Method writeMethod = pd.getWriteMethod();

                        if (field.getType() == Integer.class) {
                            writeMethod.invoke(bean, Integer.valueOf(value));
                        }
                        else if (field.getType() == Long.class) {
                            writeMethod.invoke(bean, Long.valueOf(value));
                        }
                        else if (field.getType() == Boolean.class) {
                            writeMethod.invoke(bean, Boolean.valueOf(value));
                        }
                        else if (field.getType() == Double.class) {
                            writeMethod.invoke(bean, Double.valueOf(value));
                        }
                        else if (field.getType() == Short.class) {
                            writeMethod.invoke(bean, Short.valueOf(value));
                        }
                        else if (field.getType() == Float.class) {
                            writeMethod.invoke(bean, Float.valueOf(value));
                        }
                        else if (field.getType() == Byte.class) {
                            writeMethod.invoke(bean, Byte.valueOf(value));
                        }
                        else {
                            writeMethod.invoke(bean, value);
                        }
                    }

                }

                list.add((T) bean);
            }
        }

        return list;
    }


    /**
     * 根据rowKey获取一行
     *
     * @param beanClass
     * @param rowKey
     * @return
     * @throws IOException
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws IntrospectionException
     * @throws IllegalArgumentException
     * @throws InvocationTargetException
     * @author tanguojun
     */
    public static <T extends BaseBean> T getRow(Class<T> beanClass, String rowKey) throws IOException,
            InstantiationException, IllegalAccessException, IntrospectionException, IllegalArgumentException,
            InvocationTargetException {

        // 获取bean上的注解,获取表名
        String tableName =
                ((HBaseTableNameAnnotation) beanClass.getAnnotation(HBaseTableNameAnnotation.class)).value();

        // 获取表
        Table table = getInstance().getTable(TableName.valueOf(tableName));

        Get get = new Get(rowKey.getBytes());

        BaseBean bean = null;

        Result result = table.get(get);

        if (!result.isEmpty()) {

            Field[] fields = beanClass.getDeclaredFields();

            Map<String, Field> fieldMap = new HashMap<String, Field>();

            for (Field field : fields) {

                HBaseFieldAnnotation annotation = field.getAnnotation(HBaseFieldAnnotation.class);

                if (annotation != null) {
                    fieldMap.put(annotation.familyName() + ":" + annotation.columnName(), field);
                }
            }

            bean = beanClass.newInstance();

            bean.setRowKey(rowKey);

            for (Cell cell : result.rawCells()) {
                // 获取family
                String family =
                        new String(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength(),
                            "UTF-8");
                // 获取column
                String qualifier =
                        new String(cell.getQualifierArray(), cell.getQualifierOffset(),
                            cell.getQualifierLength(), "UTF-8");
                // 获取value
                String value =
                        new String(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength(),
                            "UTF-8");

                Field field = fieldMap.get(family + ":" + qualifier);

                if (field != null) {
                    PropertyDescriptor pd = new PropertyDescriptor(field.getName(), beanClass);
                    // 获得写方法
                    Method writeMethod = pd.getWriteMethod();

                    if (field.getType() == Integer.class) {
                        writeMethod.invoke(bean, Integer.valueOf(value));
                    }
                    else if (field.getType() == Long.class) {
                        writeMethod.invoke(bean, Long.valueOf(value));
                    }
                    else if (field.getType() == Boolean.class) {
                        writeMethod.invoke(bean, Boolean.valueOf(value));
                    }
                    else if (field.getType() == Double.class) {
                        writeMethod.invoke(bean, Double.valueOf(value));
                    }
                    else if (field.getType() == Short.class) {
                        writeMethod.invoke(bean, Short.valueOf(value));
                    }
                    else if (field.getType() == Float.class) {
                        writeMethod.invoke(bean, Float.valueOf(value));
                    }
                    else if (field.getType() == Byte.class) {
                        writeMethod.invoke(bean, Byte.valueOf(value));
                    }
                    else {
                        writeMethod.invoke(bean, value);
                    }
                }
            }
        }

        return (T) bean;
    }


    /**
     * 添加列族数据
     *
     * @param tableName
     * @param rowKey
     * @param family
     * @param dataMap
     * @throws IOException
     * @author tanguojun
     */
    public static void insertByFamily(String tableName, String rowKey, String family,
            Map<String, Object> dataMap) throws IOException {

        // 获取表
        Table table = getInstance().getTable(TableName.valueOf(tableName));

        // 实例化一个put
        Put put = new Put(rowKey.getBytes());

        // 循环设置字段值
        for (String key : dataMap.keySet()) {
            put.addColumn(family.getBytes(), key.getBytes(), dataMap.get(key).toString().getBytes());
        }

        // 添加数据
        table.put(put);

        // 关闭表
        table.close();
    }


    /**
     * 删除指定列
     *
     * @param tableName
     * @param rowKey
     * @param family
     * @param columns
     * @throws IOException
     * @author tanguojun
     */
    public static void deleteByFamily(String tableName, String rowKey, String family, String... columns)
            throws IOException {

        // 获取表
        Table table = getInstance().getTable(TableName.valueOf(tableName));

        Delete delete = new Delete(rowKey.getBytes());

        for (String column : columns) {

            delete.addColumn(family.getBytes(), column.getBytes());
        }

        // 删除
        table.delete(delete);

        // 关闭
        table.close();

    }


    /**
     * 删除bean中不为空的列
     *
     * @param bean
     * @throws Exception
     * @author tanguojun
     */
    public static void deleteByBean(BaseBean bean) throws Exception {

        // 获取bean的class
        Class<? extends BaseBean> beanClass = bean.getClass();

        // 获取bean上的注解,获取表名
        String tableName =
                ((HBaseTableNameAnnotation) beanClass.getAnnotation(HBaseTableNameAnnotation.class)).value();

        // 获取表
        Table table = getInstance().getTable(TableName.valueOf(tableName));

        Delete delete = new Delete(bean.getRowKey().getBytes());

        JSONObject beanJson = new JSONObject(bean);

        // 去除主键
        beanJson.remove(ROWKEY);

        // 将json转换为map进行遍历
        Map<String, Object> dataMap = beanJson.toMap();

        // 循环设置字段值
        for (String field : dataMap.keySet()) {

            // 获取属性的family注解
            HBaseFieldAnnotation fieldAnnotation =
                    beanClass.getDeclaredField(field).getAnnotation(HBaseFieldAnnotation.class);

            // 字段含有family注解,则进行添加数据
            if (fieldAnnotation != null) {

                // 新增
                delete.addColumn(fieldAnnotation.familyName().getBytes(), field.getBytes());
            }
        }

        table.delete(delete);

        table.close();
    }


    /**
     * 删除一条(行)数据
     *
     * @param bean
     * @throws Exception
     * @author tanguojun
     */
    public static void deleteAll(Class<? extends BaseBean> beanClass, String rowKey) throws Exception {

        String tableName =
                ((HBaseTableNameAnnotation) beanClass.getAnnotation(HBaseTableNameAnnotation.class)).value();

        // 获取表
        Table table = getInstance().getTable(TableName.valueOf(tableName));

        // 删除
        table.delete(new Delete(rowKey.getBytes()));

        // 关闭
        table.close();
    }


    /**
     * 新增
     *
     * @param bean
     * @throws IOException
     * @throws SecurityException
     * @throws NoSuchFieldException
     * @author tanguojun
     */
    public static void insertByBean(BaseBean bean) throws IOException, SecurityException,
            NoSuchFieldException {
        
        // 获取bean的class
        Class<? extends BaseBean> beanClass = bean.getClass();

        // 获取bean上的注解,获取表名
        String tableName =
                ((HBaseTableNameAnnotation) beanClass.getAnnotation(HBaseTableNameAnnotation.class)).value();

        // 获取表
        Table table = getInstance().getTable(TableName.valueOf(tableName));

        // 实例化一个put
        Put put = new Put(bean.getRowKey().getBytes());

        JSONObject beanJson = new JSONObject(bean);

        // 去除主键
        beanJson.remove(ROWKEY);

        // 将json转换为map进行遍历
        Map<String, Object> dataMap = beanJson.toMap();

        // 循环设置字段值
        for (String field : dataMap.keySet()) {

            // 获取属性的注解
            HBaseFieldAnnotation fieldAnnotation =
                    beanClass.getDeclaredField(field).getAnnotation(HBaseFieldAnnotation.class);

            // 字段含有family注解,则进行添加数据
            if (fieldAnnotation != null) {

                // 新增
                put.addColumn(fieldAnnotation.familyName().getBytes(), fieldAnnotation.columnName()
                    .getBytes(), dataMap.get(field).toString().getBytes());
            }
        }

        // put
        table.put(put);

        // 关闭
        table.close();
    }


    public static void main(String[] args) throws Exception {
        Random random = new Random();
        JSONObject json = new JSONObject();
        int pronvinceId = random.nextInt(KafkaProducerDemo.PROVINCES.length);
        json.put("provinceId", pronvinceId);
        json.put("province", KafkaProducerDemo.PROVINCES[pronvinceId]);
        json.put("sex", KafkaProducerDemo.SEXS[random.nextInt(2)]);
        json.put("age", random.nextInt(80));
        json.put("time", System.currentTimeMillis());
        json.put("id", System.currentTimeMillis());
        System.out.println(json.get("id"));
        // insertUserData("account:user", "address", json.toMap());

        // insertForBean(new UserRegister());
        User u = new User();
        u.setRowKey("1480400520235");
        u.setProvince("aa");
        u.setSex("aas");

        // getRow(UserRegister.class, "1480400520235");

        // System.out.println(getRow(UserRegister.class, "1480319370890"));
        // System.out.println(scan(UserRegister.class).size());
        // insertByBean(u);
        // deleteAllForRow(UserRegister.class, "1480400520235");
        // JSONObject j = new JSONObject(u);
        // System.out.println(j);
    }

}
