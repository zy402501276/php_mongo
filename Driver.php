<?php
namespace App\Utils\MongoDB;

use EasySwoole\EasySwoole\Trigger;
use EasySwoole\SyncInvoker\AbstractInvoker;
use MongoDB\Client;
use MongoDB\BSON\ObjectId;


class Driver extends AbstractInvoker
{
    private $db;

    private $_filter_field = null; //过滤字段 ?  不需要返回的字段..
    private $_field        = null; //查询字段    select a,b,c...
    private $_opt          = null; //操作的方法  insertOne findOne这种
    private $_where_field  = null; //查询字段的搜索条件 where xx='123'
    private $_table        = null; //操作的表
    private $_options      = [];   //可选项
    private $_result       = ['state'=>400 , 'data'=>[],'msg'=>'error']; //返回结果

    //条件操作符映射字串
    private $_where_opt =[ '>'  => '$gt',  //大于
                           '>=' => '$gte', //大于等于
                           '<'  => '$lt' , //小于
                           '<=' => '$lte', //小于等于
                           '!=' => '$ne' , //不等于
                           '='  => '$eq'   //等于
                         ];

    //mongodb默认配置
    private $conf = ['host'     => '192.168.188.209',
                     'username' => null,
                     'password' => null,
                     'port'     => '27017',
                     'dbname'   => 'cloud'
                    ];


    /**
     * @desc  获取数据库客户端实例
     * @param array|null $conf
     * @return Client
     */
    function getDb( array $conf = null ):Client  //需要优化
    {
        if($this->db == null){
            $conf =   $conf ?? $this->conf;
            $this->db = new Client( $this->setConf( $conf ) );
        }
        return $this->db;
    }

    /**
     * @desc  指定操作的表
     * @param string $tableName
     * @return $this|string
     */
    public function table( string $tableName )
    {
        if( strlen( $tableName ) < 1 )
        {
            return $this->_result['msg'] = '表名不能为空';
        }

        $this->_table = $tableName;

        return $this;

    }

    /**
     * @desc  设置过滤操作字段--废弃
     * @param $fileds
     * @return Client
     */
    public function filteFields( $filtefileds )
    {
        if( !is_array( $filtefileds ) )
        {
            $filtefileds = [$filtefileds];
        }
        $this->_filter_field = $filtefileds;
        return $this;
    }

    /**
     * @desc  查询：需要的字段 相当于 select a,b,c 、、、、、
     * @param $fileds
     */
    public function field( $fileds , $isShow = 1 )
    {
        if( !is_array( $fileds ) )
        {
            $fileds = [$fileds];
        }

       return  $this->optionsSet( 'projection',null,  $fileds , $isShow );
    }

    /**
     * @desc  排序
     * @param $fileds
     */
    public function sort( $fileds , $isShow = 1 )
    {
        if( !is_array( $fileds ) )
        {
            $fileds = [$fileds];
        }
        return $this->optionsSet( 'sort',null ,$fileds , $isShow);
    }

    /**
     * @desc  限制数目
     * @param $count
     */
    public function limit( $count )
    {
        return $this->optionsSet( 'limit',$count);
    }

    /**
     * @desc  跳过数目
     * @param $count
     * @return $this
     */
    public function skip( $count )
    {
        return $this->optionsSet( 'skip',$count);
    }

    /**
     * @desc 创建查询可选项
     * @param $option
     * @param null $count
     * @param $fileds
     * @param int $isShow
     * @return $this
     */
    public function optionsSet( $option , $count = null , $fileds=null , $isShow = 1)
    {
        if( !empty($count) )  //如limit限制
        {
            $this->_options[$option] = $count;
            return $this;
        }

        foreach ( $fileds as $key => $val )
        {

            $this->_options[$option][$val] = $isShow;
        }
        return $this;
    }


    /**
     * @desc  搜索条件
     * @param $field
     * @param $opt
     * @param $value
     */
    public function where( $field , $opt = '=' , $value )
    {
        if( empty( $field ) || empty( $value ) )
        {
            return $this->_result['msg'] = '搜索条件不能为空';
        }
        if( !empty( $this->_where_field[$field] ) ) //如果有一样的搜索内容，则合并条件
        {
            $this->_where_field[$field] = array_merge( $this->_where_field[$field],[$this->_where_opt[$opt]=>$value]);
        }else{
            $this->_where_field[$field] = [$this->_where_opt[$opt]=>$value];
        }
        return $this;
    }


    /**
     * @desc 插入单条数据
     * @param $tableName string 表名
     * @param $insertData array 键值对数据
     * @return array ['state'=>400 , 'data'=>[],'msg'=>'error'];
     */
    public function insert( $insertData )
    {

        //过滤字段
        if( is_array( $this->_filter_field ) )
        {
            foreach ( $insertData as $key => $val )
            {
                if( !in_array( $key , $this->_filter_field ) ) unset( $insertData[$key] );
            }
        }
        $this->_buildInsert( $insertData , 'insertOne' );
        $this->reset();
        return $this->_result;

    }

    /**
     * 插入多条数据
     * @param $tableName
     * @param $insertData 二维数组
     * @param array $option  操作可选项 field 包含需要过滤的字段
     * @return $this
     */
    public function insertAll( $insertData , $option = [] )
    {
        $allowFields = $option['field'] ?? [];

        foreach ($insertData as $data){  //可以再单独封装一层过滤方法
            // 过滤掉不允许的字段
            if (!empty($allowFields)) {
                foreach ($data as $data_k => $data_v){
                    if (!in_array($data_v, $allowFields)){
                        unset($data[$data_k]);
                    }
                }
            }
        }

        $this->_buildInsert( $insertData, 'insertMany');
        $this->reset();
        return $this->_result;

    }

    /**
     * @desc  删除一条
     * @param $tableName
     * @param array $filter
     * @return array|string
     */
    public function delete( $tableName , array $filter , $options )
    {
        if( strlen( $tableName ) <1 || empty($filter) )
        {
            return $this->_result['msg'] = '表名或者删除条件不能为空';
        }

        $this->_table = $tableName;
        $this->_opt = "deleteOne";
        $this->_options = $options;

        $this->_buildExcute( $filter );
        $this->reset();
        return $this->_result;
    }


    /**
     * @desc  删除多条
     * @param $tableName
     * @param array $filter
     * @return array|string
     */
    public function deleteAll( $tableName , array $filter , $options )
    {
        if( strlen( $tableName ) <1 || empty($filter) )
        {
            return $this->_result['msg'] = '表名或者删除条件不能为空';
        }

        foreach ( $filter as $key => $val )
        {
            if( empty($val) ) return $this->_result['msg'] = '删除条件不能为空';
        }

        $this->_table = $tableName; //构造前缀
        $this->_opt = "deleteMany";
        $this->_options = $options;
        $this->_buildExcute( $filter );
        $this->reset();
        return $this->_result;
    }


    /**
     * @desc  搜索单条记录,可通过id查询,不需要写其他搜索条件
     * @param $tableName
     * @param $filter
     */
    public function getOne(  $id = null   )
    {

        if( !empty( $id ) )//搜索条件不为空
        {
            $this->_where_field['_id'] = new ObjectId( $id );
        }
        if( empty( $this->_where_field ) ) return $this->_result['msg'] = '搜索条件为空';

        $this->_opt = 'findOne';

        return   $this->get();

    }

    /**
     * @desc 搜索多条，需要写搜索条件
     * @return array
     */
    public function get()
    {
        $this->_opt = $this->_opt??'find';
        return $this->_bulidGet($this->_where_field);
//        $this->_buildExcute( $this->_where_field );
//        $items = [];
//        if( !empty($this->_result['data']) )
//        {
//            $items = $this->objToStr( $this->_result['data'] );
//        }
//        unset($this->_result['data']);
//
//        $this->_result['data'] =$items;
//
//        $this->reset();
//        return $this->_result;
    }

    /**
     * @desc  聚合操作 --简单封装
     * @param $data
     * @return array
     */
    public function aggregation( $data )
    {
        $this->_opt = 'aggregate';
        return $this->_bulidGet($data);
    }

    /**
     * @desc 查询操作执行过程
     * @param $data
     * @return array
     */
    private function _bulidGet($data)
    {
        $this->_buildExcute( $data );
        $items = [];
        if( !empty($this->_result['data']) )
        {
            $items = $this->objToStr( $this->_result['data'] );
        }
        unset($this->_result['data']);

        $this->_result['data'] =$items;
        $this->reset();
        return $this->_result;
    }

    /**
     * @desc  对象转字串
     * @param $data
     * @return array
     */
    private function objToStr($data)
    {
        $items = [];

        foreach ( $data as $key => $val )
        {
            if ($val instanceof ObjectID) {
                $items[ $key ] =  $val->__toString();
            }
            elseif( is_object( $val ) )
            {
                $items[ $key ]= $this->objToStr($val);
            }
            else
            {
                $items[ $key ] = $val;
            }
        }
        return $items;
    }


    /**
     * @desc  构建插入操作
     * @param $tableName
     * @param $insertData
     * @param $operation
     */
    private function _buildInsert(  $insertData , $operation ){
        $this->_opt = $operation;
        $this->_buildExcute( $insertData );
    }


    /**
     * @desc 执行数据库操作
     * @param null $tableData
     * @return bool
     */
    private function _buildExcute( $tableData = null )
    {
        $dbname = $this->conf['dbname']; //指定数据库
        $table = $this->_table;          //指定表
        $operate = $this->_opt;          //指定操作

        //*************
        //mongod原生操作
        $collection = $this->db->$dbname->$table;
        $result = $collection->$operate( $tableData , $this->_options );

        //*************

        if( strstr( $this->_opt , "insert" ) )
        {
            $optCount = "getInsertedCount";
        }
        elseif( strstr( $this->_opt , "delete" ) )
        {
            $optCount = "getDeletedCount";
        }elseif( strstr( $this->_opt , "find" ) || strstr( $this->_opt , "aggregate" ) )
        {
            if( !empty( $result ) )
            {
                $this->_result['state'] = 200;
                $this->_result['msg']   = 'SUCCESS';
                $this->_result['data']  = $result ;
            }
            else
            {
                $this->_result['msg']   = '无法查到该记录';
            }
            return true;
        }

        if( $result->$optCount() )
        {
            $this->_result['state'] = 200;
            $this->_result['msg']   = 'SUCCESS';
            $this->_result['data'] =[ 'optCount'=>$result->$optCount() ];
        }

        return true;
    }




    /**
     * @desc 创建连接配置字符
     * @param array $conf
     * @return String
     */
    private function setConf( array $conf ):String
    {
        $str = 'mongodb://';

        if( !empty( $conf['username'] ) ) $str .= $conf['username'].':';
        if( !empty( $conf['password'] ) ) $str .= $conf['password'].'@';
        if( !empty( $conf['host'] ) )     $str  .= $conf['host'];
        if( !empty( $conf['port'] ) )     $str  .= ':'.$conf['port'];
        if( !empty( $conf['dbname'] ) )   $str  .= '/'.$conf['dbname']; //无法直接指定数据库

        return $str;

    }

    /**
     * @desc 重置构造器
     * @return $this
     */
    protected function reset()
    {
        $this->_field = null;
        $this->_opt = null;
        $this->_table = null;
        return $this;
    }


    protected function onException(\Throwable $throwable)
    {
        Trigger::getInstance()->throwable($throwable);
        return null;
    }


}