import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by 乐 on 2019/12/23.
 */
public class HbaseControl {
    Admin admin;
    Connection connection;
    Configuration config;

    HbaseControl() throws IOException {
        initConfiguration();
        connection = ConnectionFactory.createConnection(config);
        admin = connection.getAdmin();
    }

    //建表
    void createTable(String tableName, List<String> columnFamily) throws
            MasterNotRunningException, ZooKeeperConnectionException, IOException {
        try {
            //判断表是否存在
            if(admin.tableExists(TableName.valueOf(tableName))){
                System.out.println("表" + tableName + "已存在");
            }else{
                //创建表属性对象,表名需要转字节
                HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
                //创建多个列族
                for(String cf : columnFamily){
                    descriptor.addFamily(new HColumnDescriptor(cf));
                }
                //根据对表的配置，创建表
                admin.createTable(descriptor);
                System.out.println("表" + tableName + "创建成功！");
            }
        }
        catch (Exception e){
            e.printStackTrace();
        }

    }
    //向表中添加列族
    void addFamily(String tableName,String familyName) throws IOException {
        Table table=connection.getTable(TableName.valueOf(tableName));
        //描述要添加的列族
        HColumnDescriptor columnDescriptor=new HColumnDescriptor(familyName);
        columnDescriptor.setMaxVersions(5);

        admin.addColumn(TableName.valueOf(tableName),columnDescriptor);
    }
    //删表
    void dropTable(String tableName) throws MasterNotRunningException,
            ZooKeeperConnectionException, IOException{
        if(admin.tableExists(TableName.valueOf(tableName))){
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
            System.out.println("表" + tableName + "删除成功！");
        }else{
            System.out.println("表" + tableName + "不存在！");
        }
    }
    //向表中插入数据（增）
    void addRowData(String tableName, String rowKey, String columnFamily, String
            column, String value) throws IOException{
        Table table= connection.getTable(TableName.valueOf(tableName));
        Put put=new Put(Bytes.toBytes(rowKey));
        put.addColumn(columnFamily.getBytes(),column.getBytes(),value.getBytes());
        table.put(put);
        table.close();
    }
    //删除多行数据（删）
    void deleteMultiRow(String tableName, List<String> rows) throws IOException{
        Table table=connection.getTable(TableName.valueOf(tableName));
        List<Delete> deleteList=new ArrayList<Delete>();
        for (String row:rows){
            Delete del=new Delete(row.getBytes());
            deleteList.add(del);
        }
        table.delete(deleteList);
        table.close();
    }
    //修改某一行的数据（改）
    void modifyValue(String tableName,String rowKey,String value){

    }
    //获取所有数据（查）
    void getAllRows(String tableName) throws IOException{
        Table table=connection.getTable(TableName.valueOf(tableName));
        Scan scan=new Scan();//无参构造函数表示获得全部row
        //迭代器
        ResultScanner scanner = table.getScanner(scan);
        //一个Result包含一行数据
        for(Result s:scanner){
            Cell[] cells = s.rawCells();
            for (Cell c:cells){
                //得到rowkey
                System.out.println("行键:" + Bytes.toString(CellUtil.cloneRow(c)));
                //得到列族
                System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(c)));
                System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(c)));
                System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(c)));
            }
        }
    }
    //获取指定行的数据（查）
    void getRow(String tableName, String rowKey) throws IOException{
        Table table=connection.getTable(TableName.valueOf(tableName));
        Get get=new Get(rowKey.getBytes());
        Result result = table.get(get);
        Cell[] cells = result.rawCells();
        for(Cell c:cells){
            System.out.println("行键:" + Bytes.toString(result.getRow()));
            System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(c)));
            System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(c)));
            System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(c)));
            System.out.println("时间戳:" + c.getTimestamp());
        }

    }
    //获得某一行指定列族：列的数据（查）
    void getRowQualifier(String tableName, String rowKey, String family, String
            qualifier) throws IOException{
        Table table=connection.getTable(TableName.valueOf(tableName));
        Get get=new Get(rowKey.getBytes());
        get.addColumn(family.getBytes(),qualifier.getBytes());
        Result result = table.get(get);
        Cell[] cells = result.rawCells();
        for(Cell c:cells){
            System.out.println("行键:" + Bytes.toString(result.getRow()));
            System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(c)));
            System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(c)));
            System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(c)));
        }
    }

    void initConfiguration() {
        config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "192.168.64.140");
        config.set("hbase.zookeeper.property.clientPort", "2181");
    }
}
