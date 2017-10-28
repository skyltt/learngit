<?php

/**
 * 数据库迁移（脚本支持库中含百万级，千万级速度很慢）
 * 1、实例化数据库操作类
 * 2、根据look函数查看原库的建表语句,并执行将表结构写入新库
 * 3、循环表名将原库的表中的数据写入新库
 * 
 */
 
 
require('./includes/initlogin.php');


//实例化数据库类
$dbs = array(
	'DB_SERVER'=>'SERVER1',
	'DB_SERVER_USERNAME'=>'USERNAME',
	'DB_SERVER_PASSWORD'=>'PASSWORD',
	'DB_DATABASE'=>'DB_DATABASE',
);

$dbnew = array(
	'NEW_DB_SERVER'=>'SERVER2',
	'NEW_DB_SERVER_USERNAME'=>'USERNAME2',
	'NEW_DB_SERVER_PASSWORD'=>'PASSWORD',
	'NEW_DB_DATABASE'=>'DB_DATABASE',
);


$db1 = new cls_mysql_operator();
$conn = $db1->connect($dbs['DB_SERVER'], $dbs['DB_SERVER_USERNAME'], $dbs['DB_SERVER_PASSWORD'], $dbs['DB_DATABASE']);

$dbnews = new cls_mysql_operator();
$conn = $dbnews->connect($dbnew['NEW_DB_SERVER'], $dbnew['NEW_DB_SERVER_USERNAME'], $dbnew['NEW_DB_SERVER_PASSWORD'], $dbnew['NEW_DB_DATABASE']);
// var_dump($dbnews);exit;


look($db1,$dbnews);

//获取原库的建表语句,并将此表结构建在新库上
function look ($db1,$dbnews){
	$database = $db1->database;
	$sql = "SELECT TABLE_NAME 
			FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '$database' ";
	$tables = $db1->fetch_records_by_sql($sql);
	$sqls = '';
	if(!empty($tables)){
		foreach ($tables as $val) {
			$table = $val['TABLE_NAME'];
			$list_sql = "SHOW CREATE TABLE $table";
			$res = $db->fetch_records_by_sql($list_sql);
			$sqls .= $res[0]['Create Table'].';</br>';
		}
		$sqlss = preg_replace('|AUTO_INCREMENT=(.*?) DEFAULT|','AUTO_INCREMENT=0 DEFAULT',$sqls);
		$dbnews->exec($sqlss);
		
		foreach($tables as $tableName) {
			getTableFied($db1,$tableName);
		}
	}
}

//获取表的字段
function getTableFied($db1,$tableName) {
	$sql = "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE table_name = '$tableName' ";
	$res = $db1->fetch_records_by_sql($sql);
	if (!empty($res)) {
		$hasId = 0;
		foreach ($res as $key => $val) {
			foreach($val as $v) {
				if($v == 'id') {
					$hasId = 1;
					continue;
				} else {
					$fields .= $v.',';
				}
			}
		}
		$fields = trim($fields,',');
	}

	getTableData ($db1,$tableName,$fields,$hasId);
}
//获取原库表数据
function getTableData ($db1,$tableName,$fields,$hasId=0) {
	
	$sql = "SELECT count(*) total FROM $tableName ";
	$total = $db1->getOneField($sql);

	if ($total>0) {
		$rows =1000;
		$offest = ceil($total/$rows);
		for ($i = 0; $i<=$offest; $i++) {
			$off = $i*$rows;
			$sql = "SELECT $fields FROM $tableName limit $off,$rows";
			$res = $db1->fetch_records_by_sql($sql);

			writeTableData ($tableName,$res,$fields,$hasId);
		}
	}
}

//将数据写入新库的表,判断是否有自增ID,有就一条条写入，没有就批量写入
function writeTableData ($tableName,$data2db,$fields,$hasId=0) {
	global $dbnews;

	if ($hasId == 0) {
		$dbnews->perform_easy($tableName, $data2db, 'INSERT','',true);
	} else {
		foreach ($data2db as $data) {
			$dbnews->perform_easy($tableName, $data, 'INSERT','',false);
		}
	}

}







?>