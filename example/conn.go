package main

import (
	"database/sql"
	"fmt"
	_ "go-aci"
	"log"
	"runtime"
	//"strings"
	//"bytes"
)

func main() {

	var db *sql.DB
	var err error

	db, err = sql.Open("aci", "test/Gepoint@192.168.219.92:2003/OSRDB")
	getError(err)
	defer db.Close()

	_, err = db.Exec("drop table if exists testcase1")
	_, err = db.Exec(`create table testcase1(col1 int, col2 varchar(256))`)
	_, err = db.Exec(`insert into testcase1(col1,col2) values (1,'ok')`)
	getError(err)

	//此处进行数据操作
	//...
	//...
	//...
}

func getError(err error) {
	_, _, line, _ := runtime.Caller(1)
	if err != nil {
		//打印调用函数行号
		fmt.Println(line)
		//获取错误信息
		log.Fatalln(err)
	}
}
