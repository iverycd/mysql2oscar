package main

import (
	"database/sql"
	"fmt"
	_ "go-aci"
	"log"
	"os"
	"runtime"
	//"strings"
	//"bytes"
)

func main() {

	var db *sql.DB
	var err error
	if len(os.Args) != 2 {
		log.Fatalln(os.Args[0] + " \nmissing conn string ,e.g: user/password@host:port/dbname")
	}

	db, err = sql.Open("aci", os.Args[1])
	getError(err)
	defer db.Close()

	_, err = db.Exec("drop table if exists testcase1")
	_, err = db.Exec(`create table testcase1(col1 int, col2 varchar(256))`)
	_, err = db.Exec(`insert into testcase1(col1,col2) values (1,'ok')`)
	getError(err)
	query, err := db.Query("select * from testcase1")
	if err != nil {
		return
	}
	defer query.Close()
	for query.Next() {
		var col1 int
		var col2 string
		err = query.Scan(&col1, &col2)
		fmt.Println("col1:", col1, "col2:", col2)
	}

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
