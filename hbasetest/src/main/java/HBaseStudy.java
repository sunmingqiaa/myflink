

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseStudy {
    private  Connection connection;
    private  Table myuser;

    @Before
    public void init() throws IOException {

        //构建conf对象
        Configuration conf = HBaseConfiguration.create();

        conf.set("hbase.zookeeper.quorum","node01:2181,node02:2181,node03:2181");

        //创建数据库的连接
       connection = ConnectionFactory.createConnection(conf);
        //System.out.println(connection);
        myuser = connection.getTable(TableName.valueOf("myuser"));

    }

    @Test
    public void  createTable() throws IOException {
        Admin admin = connection.getAdmin();
        //定义表名
        TableName tableName = TableName.valueOf("myuser");
        //定义表的描述符
        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
        //构建列族
        HColumnDescriptor f1 = new HColumnDescriptor("f1");
        HColumnDescriptor f2 = new HColumnDescriptor("f2");

        tableDescriptor.addFamily(f1);
        tableDescriptor.addFamily(f2);
        //创建表
//        admin.createTable(tableDescriptor);

        System.out.println(admin.tableExists(TableName.valueOf("myuser")));
        admin.close();

    }
    /**
     * 插入数据
     */
    @Test
    public  void  addDatas() throws IOException {
        //获取连接
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "node01");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        Connection connection = ConnectionFactory.createConnection(configuration);
        //获取表
        Table myuser = connection.getTable(TableName.valueOf("myuser"));
        //创建put对象，并指定rowkey
        Put put = new Put("0001".getBytes());
        put.addColumn("f1".getBytes(),"id".getBytes(), Bytes.toBytes(1));
        put.addColumn("f1".getBytes(),"name".getBytes(), Bytes.toBytes("张三"));
        put.addColumn("f1".getBytes(),"age".getBytes(), Bytes.toBytes(18));

        put.addColumn("f2".getBytes(),"address".getBytes(), Bytes.toBytes("地球人"));
        put.addColumn("f2".getBytes(),"phone".getBytes(), Bytes.toBytes("15874102589"));
        //插入数据
        myuser.put(put);
        //关闭表
        myuser.close();

    }


    @Test
    public void putData() throws IOException {
        //获取表操作对象
        Table myuser = connection.getTable(TableName.valueOf("myuser"));
        //创建put对象
        Put put = new Put("1".getBytes());
//        put.addColumn("f1".getBytes(),"id".getBytes(),"0001".getBytes());
//        put.addColumn("f1".getBytes(),"name".getBytes(),"zhangsan".getBytes());
//        put.addColumn("f1".getBytes(),"age".getBytes(), Bytes.toBytes(28));
//        put.addColumn("f2".getBytes(),"phone".getBytes(),"13888888888".getBytes());
        put.addColumn("f2".getBytes(),"address".getBytes(),"育新黑马".getBytes());

        myuser.put(put);

        myuser.close();
    }


    @Test
    public void insertBatchData() throws IOException {

//        //获取连接
//        Configuration configuration = HBaseConfiguration.create();
//        configuration.set("hbase.zookeeper.quorum", "node01:2181,node02:2181");
//        Connection connection = ConnectionFactory.createConnection(configuration);
        //获取表
        Table myuser = connection.getTable(TableName.valueOf("myuser"));
        //创建put对象，并指定rowkey
        Put put = new Put("0002".getBytes());
        put.addColumn("f1".getBytes(),"id".getBytes(),Bytes.toBytes(1));
        put.addColumn("f1".getBytes(),"name".getBytes(),Bytes.toBytes("曹操"));
        put.addColumn("f1".getBytes(),"age".getBytes(),Bytes.toBytes(30));
        put.addColumn("f2".getBytes(),"sex".getBytes(),Bytes.toBytes("1"));
        put.addColumn("f2".getBytes(),"address".getBytes(),Bytes.toBytes("沛国谯县"));
        put.addColumn("f2".getBytes(),"phone".getBytes(),Bytes.toBytes("16888888888"));
        put.addColumn("f2".getBytes(),"say".getBytes(),Bytes.toBytes("helloworld"));

        Put put2 = new Put("0003".getBytes());
        put2.addColumn("f1".getBytes(),"id".getBytes(),Bytes.toBytes(2));
        put2.addColumn("f1".getBytes(),"name".getBytes(),Bytes.toBytes("刘备"));
        put2.addColumn("f1".getBytes(),"age".getBytes(),Bytes.toBytes(32));
        put2.addColumn("f2".getBytes(),"sex".getBytes(),Bytes.toBytes("1"));
        put2.addColumn("f2".getBytes(),"address".getBytes(),Bytes.toBytes("幽州涿郡涿县"));
        put2.addColumn("f2".getBytes(),"phone".getBytes(),Bytes.toBytes("17888888888"));
        put2.addColumn("f2".getBytes(),"say".getBytes(),Bytes.toBytes("talk is cheap , show me the code"));


        Put put3 = new Put("0004".getBytes());
        addClumn(put3);

        Put put4 = new Put("0005".getBytes());
        put4.addColumn("f1".getBytes(),"id".getBytes(),Bytes.toBytes(4));
        put4.addColumn("f1".getBytes(),"name".getBytes(),Bytes.toBytes("诸葛亮"));
        put4.addColumn("f1".getBytes(),"age".getBytes(),Bytes.toBytes(28));
        put4.addColumn("f2".getBytes(),"sex".getBytes(),Bytes.toBytes("1"));
        put4.addColumn("f2".getBytes(),"address".getBytes(),Bytes.toBytes("四川隆中"));
        put4.addColumn("f2".getBytes(),"phone".getBytes(),Bytes.toBytes("14888888888"));
        put4.addColumn("f2".getBytes(),"say".getBytes(),Bytes.toBytes("出师表你背了嘛"));

        Put put5 = new Put("0005".getBytes());
        put5.addColumn("f1".getBytes(),"id".getBytes(),Bytes.toBytes(5));
        put5.addColumn("f1".getBytes(),"name".getBytes(),Bytes.toBytes("司马懿"));
        put5.addColumn("f1".getBytes(),"age".getBytes(),Bytes.toBytes(27));
        put5.addColumn("f2".getBytes(),"sex".getBytes(),Bytes.toBytes("1"));
        put5.addColumn("f2".getBytes(),"address".getBytes(),Bytes.toBytes("哪里人有待考究"));
        put5.addColumn("f2".getBytes(),"phone".getBytes(),Bytes.toBytes("15888888888"));
        put5.addColumn("f2".getBytes(),"say".getBytes(),Bytes.toBytes("跟诸葛亮死掐"));


        Put put6 = new Put("0006".getBytes());
        put6.addColumn("f1".getBytes(),"id".getBytes(),Bytes.toBytes(5));
        put6.addColumn("f1".getBytes(),"name".getBytes(),Bytes.toBytes("xiaobubu—吕布"));
        put6.addColumn("f1".getBytes(),"age".getBytes(),Bytes.toBytes(28));
        put6.addColumn("f2".getBytes(),"sex".getBytes(),Bytes.toBytes("1"));
        put6.addColumn("f2".getBytes(),"address".getBytes(),Bytes.toBytes("内蒙人"));
        put6.addColumn("f2".getBytes(),"phone".getBytes(),Bytes.toBytes("15788888888"));
        put6.addColumn("f2".getBytes(),"say".getBytes(),Bytes.toBytes("貂蝉去哪了"));

        List<Put> listPut = new ArrayList<Put>();
        listPut.add(put);
        listPut.add(put2);
        listPut.add(put3);
        listPut.add(put4);
        listPut.add(put5);
        listPut.add(put6);

        myuser.put(listPut);
        myuser.close();
    }

    public void addClumn(Put put3) {
        put3.addColumn("f1".getBytes(),"id".getBytes(), Bytes.toBytes(3));
        put3.addColumn("f1".getBytes(),"name".getBytes(),Bytes.toBytes("孙权"));
        put3.addColumn("f1".getBytes(),"age".getBytes(),Bytes.toBytes(35));
        put3.addColumn("f2".getBytes(),"sex".getBytes(),Bytes.toBytes("1"));
        put3.addColumn("f2".getBytes(),"address".getBytes(),Bytes.toBytes("下邳"));
        put3.addColumn("f2".getBytes(),"phone".getBytes(),Bytes.toBytes("12888888888"));
        put3.addColumn("f2".getBytes(),"say".getBytes(),Bytes.toBytes("what are you 弄啥嘞！"));
    }

    //按照rowkey获取所有列的值
    @Test
    public void  getData() throws IOException {
        //创建一个get对象，用于获取一条数据
        Get get = new Get("0006".getBytes());
        //设置过滤查询条件（列的过滤）
        get.addColumn("f1".getBytes(),"name".getBytes());
        get.addColumn("f2".getBytes(),"address".getBytes());

        //返回数据的结果集
        Result result = myuser.get(get);
        List<Cell> cells = result.listCells();
        //第一种获取方式
        /*for (Cell cell : cells) {
            byte[] rowkey = cell.getRow();
            byte[] family = cell.getFamily();
            byte[] qualifier = cell.getQualifier();
            byte[] value = cell.getValue();
            //判断数据类型是否是age或者id
            if(Bytes.toString(qualifier).equals("id")||Bytes.toString(qualifier).equals("age")){
                System.out.println("rowkey:"+Bytes.toString(rowkey)+",family:"+Bytes.toString(family)+
                        ",qualifier:"+Bytes.toString(qualifier)+",value:"+Bytes.toInt(value));
            }else {
                System.out.println("rowkey:"+Bytes.toString(rowkey)+",family:"+Bytes.toString(family)+
                        ",qualifier:"+Bytes.toString(qualifier)+",value:"+Bytes.toString(value));
            }

        }*/
        //第二种数据获取方式
       /* for (Cell cell : cells) {
            String family = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
            String qualifier = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
            String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
            System.out.println("family:"+family+
                    ",qualifier:"+qualifier+",value:"+value);

        }*/
       //第三种方式
      /* while ( result.advance()){

           Cell cell = result.current();
           String family = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
           String qualifier = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
           String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
           System.out.println("family:"+family+
                   ",qualifier:"+qualifier+",value:"+value);
       }*/

      //第四种方式
        while ( result.advance()){

            Cell cell = result.current();
            byte[] rowkey = CellUtil.cloneRow(cell);
            byte[] family = CellUtil.cloneFamily(cell);
            byte[] qualifier = CellUtil.cloneQualifier(cell);
            byte[] value = CellUtil.cloneValue(cell);
            //判断数据类型是否是age或者id
            if(Bytes.toString(qualifier).equals("id")||Bytes.toString(qualifier).equals("age")){
                System.out.println("rowkey:"+Bytes.toString(rowkey)+",family:"+Bytes.toString(family)+
                        ",qualifier:"+Bytes.toString(qualifier)+",value:"+Bytes.toInt(value));
            }else {
                System.out.println("rowkey:"+Bytes.toString(rowkey)+",family:"+Bytes.toString(family)+
                        ",qualifier:"+Bytes.toString(qualifier)+",value:"+Bytes.toString(value));
            }
        }

    }

    @Test
    public void scanData() throws IOException {
        //进行全表扫描
        Scan scan = new Scan();
        //通过start  heend 进行扫描
        //Scan scan = new Scan("0001".getBytes(),"0005".getBytes());

        scan.setStartRow("0001".getBytes());
        scan.setStopRow("0005".getBytes());
        ResultScanner scanner = myuser.getScanner(scan);
        //遍历获取每一个结果集
        for (Result result : scanner) {

            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                byte[] rowkey = CellUtil.cloneRow(cell);
                byte[] family = CellUtil.cloneFamily(cell);
                byte[] qualifier = CellUtil.cloneQualifier(cell);
                byte[] value = CellUtil.cloneValue(cell);
                //判断数据类型是否是age或者id
                if(Bytes.toString(qualifier).equals("id")||Bytes.toString(qualifier).equals("age")){
                    System.out.println("rowkey:"+Bytes.toString(rowkey)+",family:"+Bytes.toString(family)+
                            ",qualifier:"+Bytes.toString(qualifier)+",value:"+Bytes.toInt(value));
                }else {
                    System.out.println("rowkey:"+Bytes.toString(rowkey)+",family:"+Bytes.toString(family)+
                            ",qualifier:"+Bytes.toString(qualifier)+",value:"+Bytes.toString(value));
                }
            }
        }
    }

    @Test
    public void getDataByFilter() throws IOException {
        Scan scan = new Scan();

        //1.rowfilter
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.LESS, new BinaryComparator("0003".getBytes()));
        //2.familyFilter
        FamilyFilter familyFilter = new FamilyFilter(CompareFilter.CompareOp.LESS, new SubstringComparator("f2"));
        //3.qualifierFilter
        //QualifierFilter qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator("name".getBytes()));
        //ValueFilter
        //ValueFilter valueFilter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("8"));

        //5.SingleColumnValueFilter
        //SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter("f1".getBytes(), "name".getBytes(), CompareFilter.CompareOp.EQUAL, "刘备".getBytes());
        //6.PrefixFilter
        //PrefixFilter prefixFilter = new PrefixFilter("00".getBytes());
        //7.通过filterlist设置多过滤器
        FilterList filterList = new FilterList();
        filterList.addFilter(rowFilter);
        filterList.addFilter(familyFilter);
        //用于设置过滤器
        scan.setFilter(filterList);

        ResultScanner scanner = myuser.getScanner(scan);
        for (Result result : scanner) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                byte[] rowkey = CellUtil.cloneRow(cell);
                byte[] family = CellUtil.cloneFamily(cell);
                byte[] qualifier = CellUtil.cloneQualifier(cell);
                byte[] value = CellUtil.cloneValue(cell);
                //判断数据类型是否是age或者id
                if(Bytes.toString(qualifier).equals("id")||Bytes.toString(qualifier).equals("age")){
                    System.out.println("rowkey:"+Bytes.toString(rowkey)+",family:"+Bytes.toString(family)+
                            ",qualifier:"+Bytes.toString(qualifier)+",value:"+Bytes.toInt(value));
                }else {
                    System.out.println("rowkey:"+Bytes.toString(rowkey)+",family:"+Bytes.toString(family)+
                            ",qualifier:"+Bytes.toString(qualifier)+",value:"+Bytes.toString(value));
                }
            }
        }

    }


    @Test
    public void getDataByPageFilter() throws IOException {
        int pagsize = 2;
        int pagnum = 3;
        //如果只有一页的情况下
        if(pagnum==1){
            Scan scan = new Scan();
            scan.setFilter(new PageFilter(pagsize));
            scan.setMaxResultSize(pagsize);
            ResultScanner scanner = myuser.getScanner(scan);
            for (Result result : scanner) {
                List<Cell> cells = result.listCells();
                for (Cell cell : cells) {
                    System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
        }else {
            //首先获取第三页的rowkey
            //可以分一页 ，每页显示5条，直接获取最后一条rowkey
            Scan scan1 = new Scan();
            scan1.setStartRow("".getBytes());//rowkey的默认值
            scan1.setFilter(new PageFilter((pagnum-1)*pagsize + 1));
            scan1.setMaxResultSize((pagnum-1)*pagsize + 1);
            ResultScanner scanner = myuser.getScanner(scan1);
            //定义startRow接收变量
            String startRowkey="";
            for (Result result : scanner) {
                List<Cell> cells = result.listCells();
                for (Cell cell : cells) {
                    byte[] rowkey = CellUtil.cloneRow(cell);
                    startRowkey = Bytes.toString(rowkey);
                }
            }
            //System.out.println(startRowkey);
            //基于startrowkey进行第三页的分页
            Scan scan2 = new Scan();
            scan2.setStartRow(startRowkey.getBytes());
            scan2.setFilter(new PageFilter(pagsize));
            scan2.setMaxResultSize(pagsize);
            ResultScanner scanner1 = myuser.getScanner(scan2);
            for (Result result : scanner1) {
                List<Cell> cells = result.listCells();
                for (Cell cell : cells) {
                    System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }

        }



    }


    @Test
    public void deleteData() throws IOException {

        Delete delete = new Delete("0001".getBytes());
        myuser.delete(delete);
    }


    @Test
    public void dropTable() throws IOException {

        Admin admin = connection.getAdmin();
        if(!admin.isTableDisabled(TableName.valueOf("user1"))){
            admin.disableTable(TableName.valueOf("user1"));
        }
        if (admin.tableExists(TableName.valueOf("user1"))){
            admin.deleteTable(TableName.valueOf("user1"));
        }

        admin.close();
    }

    @After
    public void close() throws IOException {

        myuser.close();
        connection.close();

    }

}
