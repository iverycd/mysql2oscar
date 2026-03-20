/*
 * stoi.h
 *  ShenTong object interface with backend import thread pool
 *  Created on: Sep 3, 2012
 *      Author: root
 */

#ifndef STOI_H_
#define STOI_H_

#ifdef __cplusplus
extern "C" {
#endif

typedef struct ACIStoiEnv					ACIStoiEnv;					/* ACI Stoi environment handle */
typedef struct ACIStoiObj					ACIStoiObj;					/* ACI Stoi object handle */
typedef struct ACIStoiObjField				ACIStoiObjField;			/* ACI Stoi object Field handle */
typedef struct ACIStoiObjColInfo			ACIStoiObjColInfo;			/* ACI Stoi object column info handle */
typedef struct ACIStoiPutDataHandle			ACIStoiPutDataHandle;		/* ACI Stoi object put data handle */


#include "acitype.h"

/*------------------------STOI Error Return Values--------------------------------*/
#define STOI_SUCCESS		(0)
#define STOI_ERROR			(-1)
#define STOI_LEN_ERROR      (-2)
#define ENCODE_ERROR		(1)

/*------------------------STOI CHARSET Values--------------------------------*/
#define STOI_ATTR_SCHARSET_NAME 1
#define STOI_ATTR_DCHARSET_NAME 2
#define STOI_ATTR_SCHARSET_PAGE 3
#define STOI_ATTR_DCHARSET_PAGE 4

#define STOI_MAX_BULKTHREAD_COUNT	1024
#define STOI_MAX_NAME_LEN	256
#define STOI_MAX_NODE_COUNT	1024
#define STOI_MAX_NODE_HASH_BUCKET_COUNT	65535
#define STOI_MAX_FILE_COUNT	STOI_MAX_BULKTHREAD_COUNT
#define STOI_MAX_TEXT_LENGTH 150000			//kstore宽列text最大字节数
#define STOI_DEFAULT_BULKTHREAD_COUNT   4
#define STOI_DEFAULT_DB_RETRY_COUNT     0x0FFFFFFF
#define STOI_DEFAULT_DB_RETRY_INTERVAL  1000
#define STOI_MAX_ROW_LEN 1024000
#define STOI_MAX_COLUMN_COUNT 20000			//kstore宽表最大列数
#define STOI_MAX_CHAR_LENGTH 8000
#define STOI_DEFAULT_LOB_LENGTH 300003		//kstore宽列binary,varbinary最大字节数
#define STOI_MAX_LOB_LENGTH 2147483647
#define STOI_DEFAULT_LOB_LENGTH_NEW 100*1024*1024

#define STOI_MAX_URL_LEN   256
#define STOI_MAX_HOST_LEN  256
#define STOI_MAX_PORT_LEN  128
#define STOI_MAX_DBNAME_LEN  256

#define STOI_DEFAULT_OBJ_BUCKET_COUNT   2
#define STOI_DEFAULT_OBJ_BULK_TIMEOUT   (5*60)
#define STOI_DEFAULT_OBJ_BULK_ROWCOUNT  60000
#define STOI_DEFAULT_OBJ_BULKBUF_SIZE   (30*1024*1024)

// 环境句柄属性类型定义

#define STOI_ENV_ATTR_HOST          0x01
#define STOI_ENV_ATTR_PORT          0x02              //传入整数的方式设置端口号
#define STOI_ENV_ATTR_DBNAME        0x03
#define STOI_ENV_ATTR_USERNAME      0x04
#define STOI_ENV_ATTR_PASSWD        0x05
#define STOI_ENV_ATTR_THREADCOUNT   0x06
#define STOI_ENV_ATTR_PACKSIZE      0x07
#define STOI_ENV_ATTR_URL           0x08	//格式：localhost:2003/osrdb
#define STOI_ENV_ATTR_DBTYPE        0x10
#define STOI_ENV_ATTR_DB_RETRY      0x20
#define STOI_ENV_ATTR_DB_RETRY_COUNT    0x21
#define STOI_ENV_ATTR_DB_RETRY_INTERVAL 0x23
#define STOI_ENV_ATTR_PORT_A        0x24              //传入字符串的方式设置端口号
#define STOI_ENV_ATTR_TLOCK_TIMES		0x25
#define STOI_ENV_ATTR_TIMESTAMP_FORMAT    	0x26

#define STOI_ENV_ATTR_DB_CHARSET_CONVERT    0x65
#define STOI_ENV_ATTR_DATA_CHARSET_ID       0x66
#define STOI_ENV_ATTR_ENV_CHARSET_ID        0x67
#define STOI_ENV_ATTR_CHARSET_CONVERT       0x68
#define STOI_ENV_ATTR_SCHARSET_CODEPAGE     0x69
#define STOI_ENV_ATTR_SCHARSET_NAME       	0x70
#define STOI_ENV_ATTR_DCHARSET_CODEPAGE     0x71
#define STOI_ENV_ATTR_DCHARSET_NAME       	0x72

#define STOI_ENV_ATTR_FILE_TYPE       		0x73
#define STOI_ENV_ATTR_FILE_PATH_LIST       	0x74
#define STOI_ENV_ATTR_FILE_PATH_COUNT       0x75
#define STOI_ENV_ATTR_FILE_ENABLE_ESCAPE    0x76
#define STOI_ENV_ATTR_FILE_ESCAPE_STR    	0x77
#define STOI_ENV_ATTR_FILE_COL_SPLIT_STR    0x78
#define STOI_ENV_ATTR_FILE_ROW_SPLIT_STR    0x79
#define STOI_ENV_ATTR_SOURCE_COL_COUNT    	0x80

#define STOI_ENV_ATTR_FILE_WRITE_MODE    	0x81
#define STOI_ENV_ATTR_CRYPTOGRAPHIC_KEY    	0x82

#define STOI_ENV_ATTR_FILE_PATH    			1
#define STOI_ENV_ATTR_FILE_DIR      		2

// 环境句柄属性值定义
#define STOI_ENV_ATTR_CLUSTER       0x01
#define STOI_ENV_ATTR_KSTORE        0x02
#define STOI_ENV_ATTR_OSCAR         0x03

#define STOI_ENV_ATTR_CLUDEVNULL    0x07
#define STOI_ENV_ATTR_KSTDEVNULL    0x08
#define STOI_ENV_ATTR_OSCDEVNULL    0x09

#define STOI_ENV_ATTR_OSCAR_RANDOM  0x10
#define STOI_ENV_ATTR_KSTORE_RANDOM 0x11
#define STOI_ENV_ATTR_TEXT_FILE 0x12
#define STOI_ENV_ATTR_BINARY_FILE 0x13
#define STOI_ENV_ATTR_OSCAR_HASH  0x14
#define STOI_ENV_ATTR_KSTORE_HASH 0x15

// 对象句柄属性类型定义
#define STOI_OBJ_ATTR_HASH_BUCKET   0x01
#define STOI_OBJ_ATTR_BULK_TIMEOUT  0x02
#define STOI_OBJ_ATTR_BULK_ROWCOUNT 0x03
#define STOI_OBJ_ATTR_BULK_SIZE     0x04
#define STOI_OBJ_ATTR_STORAGE_TYPE  0x05
#define STOI_OBJ_ATTR_BULKBUF_TYPE  0x21
#define STOI_OBJ_ATTR_IMP_ROW_COUNT 0x22
#define STOI_OBJ_ATTR_SCHEDULER_POLLING_INTERVARL 0x23

#define STOI_OBJ_ATTR_NODE_COUNT  0x06
#define STOI_OBJ_ATTR_HASH_KEY_INDEX  0x07
#define STOI_OBJ_ATTR_HASH_BUCKET_VAL_LIST  0x08
#define STOI_OBJ_ATTR_HASH_NODE_ID_LIST  0x09
#define STOI_OBJ_ATTR_HASH_BUCKET_INDEX  0x10
#define STOI_OBJ_ATTR_HASH_BUCKET_COUNT  0x11
#define STOI_OBJ_ATTR_HASH_SLICE_ID_LIST  0x12
#define STOI_OBJ_ATTR_SLICE_COUNT  0x13
#define STOI_OBJ_ATTR_TEXT_MAXLEN   0x14

// 对象存储类型定义
#define STOI_OBJ_STORAGE_TYPE_HASH          0x01
#define STOI_OBJ_STORAGE_TYPE_ROUND_ROBIN   0x02
#define STOI_OBJ_STORAGE_TYPE_REPLICATED    0x03
#define STOI_OBJ_STORAGE_TYPE_SINGLE_NODE   0x04
#define STOI_OBJ_STORAGE_TYPE_HASH_AGG	0x05

#define STOI_OBJ_BULKBUF_TYPE_SINGLE	0x06
#define STOI_OBJ_BULKBUF_TYPE_ROUND		0x07

// 字段句柄属性类型定义
#define STOI_FIELD_ATTR_PRECISION       0x01
#define STOI_FIELD_ATTR_SCALE           0x02
#define STOI_FIELD_SEQ_NAME             0x03
#define STOI_FIELD_SEQ_START            0x04
#define STOI_FIELD_SEQ_INCREASE         0x05

// 字段类型定义
//// 整数相关类型
#define STOI_FDTYPE_INT8        0x01                    // 有符号单字节整形
#define STOI_FDTYPE_TINYINT     STOI_FDTYPE_INT8
#define STOI_FDTYPE_UINT8       0x02                    // 无符号单字节整形数据
#define STOI_FDTYPE_UCHAR       STOI_FDTYPE_UINT8
#define STOI_FDTYPE_UTINYINT    STOI_FDTYPE_UCHAR
#define STOI_FDTYPE_INT16       0x03                    // 有符号2字节整形
#define STOI_FDTYPE_SHORT       STOI_FDTYPE_INT16
#define STOI_FDTYPE_UINT16      0x04                    // 无符号2字节整形
#define STOI_FDTYPE_USHORT      STOI_FDTYPE_UINT16
#define STOI_FDTYPE_INT32       0x05                    // 有符号4字节整形
#define STOI_FDTYPE_INT         STOI_FDTYPE_INT32
#define STOI_FDTYPE_UINT32      0x06                    // 无符号4字节整形
#define STOI_FDTYPE_UINT         STOI_FDTYPE_UINT32
#define STOI_FDTYPE_INT64       0x07                    // 有符号4字节整形
#define STOI_FDTYPE_UINT64      0x08                    // 无符号4字节整形

//// 浮点数相关类型
#define STOI_FDTYPE_FLOAT8      0x11                    // 单精度浮点数
#define STOI_FDTYPE_FLOAT16     0x12                    // 双精度浮点数
#define STOI_FDTYPE_DOUBLE      STOI_FDTYPE_FLOAT16
#define STOI_FDTYPE_DECIMAL     0x13                    // 十进制高精度浮点数
#define STOI_FDTYPE_NUMERIC     STOI_FDTYPE_DECIMAL
#define STOI_FDTYPE_NUMBER      STOI_FDTYPE_DECIMAL

//// 字符串相关类型
#define STOI_FDTYPE_TEXT        0x21                    // 静态分配的变长字符串类型：输入为定长字符数组，数组长度为字段定义长度，并以\0结尾
#define STOI_FDTYPE_VARCHAR     STOI_FDTYPE_TEXT
#define STOI_FDTYPE_CHAR        STOI_FDTYPE_TEXT        // 静态分配的定长字符串类型：输入为定长字符数组，数组长度为字段定义长度，并以\0结尾
#define STOI_FDTYPE_MTEXT       0x22                    // 动态分配的变长字符串类型：输入为char *类型，存储的数据以\0结尾
#define STOI_FDTYPE_MVARCHAR    STOI_FDTYPE_MTEXT
#define STOI_FDTYPE_MCHAR       STOI_FDTYPE_MTEXT       // 动态分配的定长字符串类型：输入为char *类型，存储的数据以\0结尾
#define STOI_FDTYPE_VARBINARY   0x23                    // 变长的二进制字符串

//// 日期时间相关类型
#define STOI_FDTYPE_TIME        0x30                    // 时间类型：输入为距00:00:00的毫秒数: int4
#define STOI_FDTYPE_DATE        0x31                    // 日期类型：输入为距1970-01-01的天数: int4
#define STOI_FDTYPE_TIMESTAMP   0x32                    // 时戳类型：输入为距1970-01-01的毫秒数: int8
#define STOI_FDTYPE_DATETIME    STOI_FDTYPE_TIMESTAMP   // 同上
#define STOI_FDTYPE_INTERVAL    0x33                    // 时间间隔：输入为时间间隔允许格式字符串，必须以\0结尾: char[100]

//// 判定类型
#define STOI_FDTYPE_BOOL        0x40                    // 布尔类型：输入为单字节整形（char）,1为真,0为假

//// 自动生成键相关类型
#define STOI_FDTYPE_AUTO_SEQ    0x50                    // 序列类型：此列不输入，由后台进程自动生成

#define STOI_BACKUP_RETRY_FAILED  -1                    //备份文件导入失败

#define STOI_FILE_WRITE_MODE_DEFAULT     (1)
#define STOI_FILE_WRITE_MODE_APPEN       (2)
#define STOI_FILE_WRITE_MODE_OVERWRITE   (3)

#define STOI_OBJ_MAX_BUCKET_COUNT STOI_MAX_BULKTHREAD_COUNT
#define STOI_MAX_SESSION_COUNT STOI_MAX_BULKTHREAD_COUNT
#define STOI_MAX_OBJ_COUNT 10000

//字符集宏定义
#define STOI_UTF8               "UTF8" 
#define STOI_UTF_8              "UTF-8" 
#define STOI_UTF_16LE           "UTF-16LE" 
#define STOI_GB18030            "GB18030" 

typedef void (*FnStoiImpErrorCallback)(dvoid * hCbHandle, acitext* szTableName, acitext* sRowData,
        sb4 dwDataSize, sb4 dwRowCount, sb4 dwColCount, acitext* szErrorMessage);

//环境句柄初始化
sword ACIStoiAllocEnv(ACIStoiEnv ** ppStoiEnv);
sword ACIStoiInitEnv(ACIStoiEnv* pStoiEnv);
sword ACIStoiEnvAttrSet(ACIStoiEnv* pStoiEnv, sb4 dwAttrType, dvoid * pValue, sb4 dwLen);
sword ACIStoiTermEnv(ACIStoiEnv* pStoiEnv);
void  ACIStoiFreeEnv(ACIStoiEnv* pStoiEnv);

//object对象相关的函数
sword ACIStoiInitObject(ACIStoiEnv* pStoiEnv, acitext* szTableName, ACIStoiObj** ppStoiObj);
sword ACIStoiObjAttrSet(ACIStoiObj* pStoiObj, sb4 dwAttrType, dvoid * pValue, sb4 dwLen);
sword ACIStoiEndInitObject(ACIStoiObj* pStoiObj);
void  ACIStoiFreeObj(ACIStoiObj* pStoiObj);

//添加列/域
sword ACIStoiObjAddField(ACIStoiObj* pStoiObj, acitext* szFieldName, ub4 dwFieldType, ub4 dwLength, ACIStoiObjField** ppStoiObjField);
sword ACIStoiFieldAttrSet(ACIStoiObjField* pStoiObjField, sb4 dwAttrType, dvoid * pValue, sb4 dwLen);

//添加数据函数
sword ACIStoiObjPutData (ACIStoiObj* pStoiObj, dvoid * hRowData);
sword ACIStoiObjPutData2(ACIStoiObj* pStoiObj, dvoid * hRowData, sb4* pColListLen);
sword ACIStoiObjPutDataNew(ACIStoiObj* pStoiObj, dvoid * hRowData, dvoid * hPutHandle, sb4* pColListLen);


//数据flush
sword ACIStoiForceFlushAllCache(ACIStoiEnv* pStoiEnv);
sword ACIStoiForceFlushObjCache(ACIStoiObj* pStoiObj);
sword ACIStoiWaitForThreadCacheToDb(ACIStoiObj* pStoiObj);

//初始化错误句柄
sword ACIStoiInitErrorHandle(ACIStoiObj* pStoiObj, FnStoiImpErrorCallback fnErrCb, dvoid * hCbHandle);

//二进制转16进制字符串函数
sword ACIStoiBinToHexStr(acitext *lhs, ub4 lhs_size, const acitext *rhs, ub4 rhs_len);

//获得列信息
sword ACIStoiGetColInfo(ACIStoiEnv* pStoiEnv, ACIStoiObj* pStoiObj, ACIStoiObjColInfo** ppObjColumnInfo);
sword ACIStoiGetColInfoE(ACIStoiEnv* pStoiEnv, ACIStoiObj* pStoiObj, ACIStoiObjColInfo** ppObjColumnInfo, acitext* szErrInfo, ub4 dwErrInfoLen);
sword ACIStoiGetCharsetInfoE(ACIStoiEnv* pStoiEnv, ACIStoiObj* pStoiObj, ACIStoiObjColInfo** ppObjColumnInfo, acitext* szErrInfo, ub4 dwErrInfoLen);
sword ACIStoiGetCharsetName(ACIStoiObjColInfo* pObjColumnInfo, acitext* szDbEncoding);
sword ACIStoiGetCharsetCode(ACIStoiObjColInfo* pObjColumnInfo, ub4* dwDbEncodingCode);
sword ACIStoiGetTableType(ACIStoiObjColInfo* pObjColumnInfo, acitext* szTabType, sb4 dwLength);
sword ACIStoiGetColCount(ACIStoiObjColInfo* pObjColumnInfo, ub4* dwCount);
sword ACIStoiGetColName(ACIStoiObjColInfo* pObjColumnInfo, sb4 index, acitext* szColumnName, sb4 dwLength);
sword ACIStoiGetColLenByIndex(ACIStoiObjColInfo* pObjColumnInfo, sb4 index, ub4* pdwLength);
sword ACIStoiGetColTypeByIndex(ACIStoiObjColInfo* pObjColumnInfo, sb4 index, ub4* pdwType);
sword ACIStoiGetColLenByName(ACIStoiObjColInfo* pObjColumnInfo, acitext* szColumnName, ub4* pdwLength);

sword ACIStoiGetTabNodeCount(ACIStoiObjColInfo* pObjColumnInfo, ub4* pDbNodeCount);
sword ACIStoiGetTabSliceCount(ACIStoiObjColInfo* pObjColumnInfo, ub4* pDbSliceCount);
sword ACIStoiGetTabHashBucketCount(ACIStoiObjColInfo* pObjColumnInfo, ub4* pDbTabHashBucketCount);
sword ACIStoiGetTabHashKey(ACIStoiObjColInfo* pObjColumnInfo, acitext* szDbTabHashKey);
sword ACIStoiGetTabHashMap(ACIStoiObjColInfo* pObjColumnInfo, sb4 index, ub4* pHashBucketVal, ub4* pNodeId, ub4* pSliceId);

void  ACIStoiFreeColInfo(ACIStoiObjColInfo* pObjColumnInfo);

sword ACIStoiAllocPutDataHadle(ACIStoiObj* pStoiObj, ACIStoiPutDataHandle** pphPutHandle);
sword ACIStoiFreePutDataHadle(ACIStoiObj* pStoiObj, ACIStoiPutDataHandle* phPutHandle);


#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif /* STOI_H_ */
