/*
本文件是用于兼容神通onet驱动，用于替代onet驱动,onet是对libpq的兼容。
onet驱动是基于libpq驱动进行修改，自研率不高，即将淘汰。
版权：天津神舟通用数据技术有限公司
作者：刘勇生
*/
#ifndef ONET_FE_H
#define ONET_FE_H

#ifdef __cplusplus
extern "C" {
#endif

#define COMPILE_ACI_LIB

#include <stdio.h>
#include "aci.h"

//-------- 对外返回的[结果/参数]的数据库类型 ----------
#define	ONET_DB_UNKNOWN_TYPE	    0

	// 数值类型
#define ONET_DB_NUMERIC          2
#define ONET_DB_DECIMAL          3
#define ONET_DB_INTEGER          4
#define ONET_DB_SMALLINT         5
#define ONET_DB_FLOAT            6
#define ONET_DB_REAL             7
#define ONET_DB_DOUBLE           8
#define ONET_DB_BIGINT           (-5)
#define ONET_DB_TINYINT          (-6)
#define ONET_DB_INTEGER4         (-9)


	// 字符类型
#define ONET_DB_CHAR             1
#define ONET_DB_VARCHAR          12
#define ONET_DB_CLOB             (-1)

	// 时间日期类型
#define ONET_DB_DATE             9
#define ONET_DB_TIME             10
#define ONET_DB_TIMESTAMP        11

	// 二级制数据类型
#define ONET_DB_BINARY           (-2)
#define ONET_DB_VARBINARY        (-3)
#define ONET_DB_BLOB             (-4)
#define ONET_DB_BIT              (-7)

//--------- 对外返回的[参数/结果]的C类型 ------------
#define OSC_C_DEFAULT           99

#define OSC_C_SIGNED_OFFSET     (-20)
#define OSC_C_UNSIGNED_OFFSET   (-22)
#define OSC_DB_INTEGER4         (-9)

// 数值类型
#define OSC_C_LONG              ONET_DB_INTEGER
#define OSC_C_SHORT             ONET_DB_SMALLINT
#define OSC_C_FLOAT             ONET_DB_REAL
#define OSC_C_DOUBLE            ONET_DB_DOUBLE
#define	OSC_C_NUMERIC		    ONET_DB_NUMERIC
#define OSC_C_SBIGINT	        (ONET_DB_BIGINT+OSC_C_SIGNED_OFFSET)
#define OSC_C_SLONG             (OSC_C_LONG+OSC_C_SIGNED_OFFSET)
#define OSC_C_SSHORT            (OSC_C_SHORT+OSC_C_SIGNED_OFFSET)
#define OSC_C_ULONG             (OSC_C_LONG+OSC_C_UNSIGNED_OFFSET)
#define OSC_C_USHORT            (OSC_C_SHORT+OSC_C_UNSIGNED_OFFSET)
#define OSC_C_TINYINT           ONET_DB_TINYINT
#define OSC_C_STINYINT          (ONET_DB_TINYINT+OSC_C_SIGNED_OFFSET)
#define OSC_C_BIT               ONET_DB_BIT
#define OSC_C_INTEGER           OSC_DB_INTEGER4
#define OSC_C_SINTEGER          (OSC_DB_INTEGER4+OSC_C_SIGNED_OFFSET)
#define OSC_C_UINTEGER          (OSC_DB_INTEGER4+OSC_C_UNSIGNED_OFFSET)

// 字符类型
#define OSC_C_CHAR              ONET_DB_CHAR
#define OSC_C_CLOB              ONET_DB_CLOB

// 时间日期类型
#define OSC_C_DATE              ONET_DB_DATE
#define OSC_C_TIME              ONET_DB_TIME
#define OSC_C_TIMESTAMP         ONET_DB_TIMESTAMP

// 二进制类型
#define OSC_C_BINARY            ONET_DB_BINARY
#define OSC_C_BLOB              ONET_DB_BLOB

	//--------- 对外返回的[参数/结果]的C类型（结束）------------
#define OSC_NTS					(-3)
#define OSC_NULL				(-1)

#define OSC_PARAM_INPUT		0

#define HASHMAP_MAX_ITEMS    1000

#define STMT_UNKNOWN_STATEMENT      0
#define STMT_SELECT_CURSOR          1
#define STMT_DLL                    5

typedef unsigned int onet_Oid;

//! 定义无效的OID
#ifndef InvalidOid
#define InvalidOid		((onet_Oid) 0)
#endif

#ifdef WIN32
typedef unsigned __int64        OUint64_t;      // 默认类型前缀为lw
typedef unsigned __int64		LOid;			// 默认类型前缀为li
typedef signed	__int64			OSint64_t;
#else
typedef unsigned long long      OUint64_t;       // 默认类型前缀为lw
typedef unsigned long long		LOid;
typedef signed long long		OSint64_t;
#endif 

/*PG libpq兼容*/
typedef enum
{
	CONNECTION_OK,					//  连接成功
	CONNECTION_BAD,					//  连接失败
	CONNECTION_STARTED,				//  等待连接建立
	CONNECTION_MADE,				//  连接建立成功，等待发送
	CONNECTION_AWAITING_RESPONSE,	//  等待服务器端的响应
	CONNECTION_AUTH_OK,				//  等待后端验证结果
	CONNECTION_SETENV				//  设置环境变量
} ConnStatusType;

typedef enum
{
	OSCAR_EMPTY_QUERY = 0,		//  空查询
	OSCAR_COMMAND_OK,			//  不需要返回结果的查询命令成功执行
	OSCAR_TUPLES_OK,			//  返回结果的查询命令成功执行,结果保存在OCresult 中
	OSCAR_COPY_OUT,				//  执行copy out 操作
	OSCAR_COPY_IN,				//  执行copy in操作
	OSCAR_BAD_RESPONSE,			//  获得以外的相应
	OSCAR_NONFATAL_ERROR,		//  执行出现非严重的错误
	OSCAR_FATAL_ERROR,			//  执行出现严重的错误
	OSCAR_MEMORY_ERROR	        //  执行出现内存不足的错误
} ExecStatusType;

/*!
@brief	连接状态机枚举结构

定义连接状态机枚举结构:
*/
typedef enum
{
	OSCAR_POLLING_FAILED = 0,   // polling失败
	OSCAR_POLLING_READING,		// 进行读操作
	OSCAR_POLLING_WRITING,		// 进行写操作
	OSCAR_POLLING_OK,           // polling 成功
	OSCAR_POLLING_ACTIVE		// 能够立即调用poll函数.
} OSCARPollingStatusType;

typedef enum _oc_lob_type_e {
	BLOB_TYPE,
	CLOB_TYPE,
}OCLobType;

typedef void* OCLobHandle;

typedef struct StmtClass* StmtClassP_t;

typedef struct StmtClass StmtClass;
typedef struct StmtClass* OCcommand;

typedef struct oc_conn oc_conn;
typedef struct oc_result oc_result;

typedef struct oc_conn OCconn;
typedef struct oc_result OCresult;
typedef struct _OSCconnOption OSCconnOption;

typedef struct _OSCprintOpt OSCprintOpt;

typedef struct tagArg		Arg_t;
typedef struct tagArg	*	ArgP_t;

typedef struct ocNotify OCnotify;

#define PostgresPollingStatusType	OSCARPollingStatusType
#define PGRES_POLLING_FAILED		OSCAR_POLLING_FAILED
#define PGRES_POLLING_READING		OSCAR_POLLING_READING
#define PGRES_POLLING_WRITING		OSCAR_POLLING_WRITING
#define PGRES_POLLING_OK			OSCAR_POLLING_OK
#define PGRES_POLLING_ACTIVE		OSCAR_POLLING_ACTIVE
#define ExecStatusType				ExecStatusType
#define PGRES_EMPTY_QUERY			OSCAR_EMPTY_QUERY
#define PGRES_COMMAND_OK			OSCAR_COMMAND_OK
#define PGRES_TUPLES_OK				OSCAR_TUPLES_OK
#define PGRES_COPY_OUT				OSCAR_COPY_OUT
#define PGRES_COPY_IN				OSCAR_COPY_IN
#define PGRES_BAD_RESPONSE			OSCAR_BAD_RESPONSE
#define PGRES_NONFATAL_ERROR		OSCAR_NONFATAL_ERROR
#define PGRES_FATAL_ERROR			OSCAR_FATAL_ERROR

#define PGconn		OCconn
#define PGresult	OCresult

typedef void(*OSCnoticeProcessor) (void *arg, const char *message);

/*
- 此部分接口为兼容onet接口所定义，同时也能兼容libpq接口
- 所有数据类型都是通过字符串获取的
*/
int				ONETInit();
int				ONETCleanUp();

OCconn*			OSCconnectdb(const char * conninfo);
OCconn*			OSCconnectStart(const char * conninfo);
OCconn*			OSCsetdbLogin(const char *ochost, const char *ocport, const char *ocoptions,
						  const char *octty, const char *dbName, const char *login,
						  const char *pwd, int isosauth);
void			OSCfinish(OCconn *conn);
ConnStatusType	OSCstatus(const OCconn *conn);
char*			OSCSQLState(OCconn * conn);
char *			OSCerrorMessage(const OCconn *conn);
OCresult *		OSCexec(OCconn *pConn, const char *query);
ExecStatusType	OSCresultStatus(const OCresult *res);
char *			OSCcmdStatus(OCresult *res);
LOid			OSCoidValue(const OCresult *res);
LOid			OSCcmdTuples(OCresult *res);
char *			OSCoidStatus(const OCresult *res);
void			OSCclear(OCresult *res);
char *			OSCgetvalue(OCresult *res, int tup_num, int field_num);
int				OSCntuples(const OCresult *result);
int				OSCnfields(const OCresult *result);
char *			OSCfname(const OCresult *result, int field_num);
int				OSCfnumber(const OCresult *result, const char *field_name);
onet_Oid		OSCftype(const OCresult *result, int field_num);
int				OSCfsize(const OCresult *result, int field_num);
int				OSCfmod(const OCresult *res, int field_num);
OCresult *		OSCprepare(OCconn *pConn, const char *stmtName, const char *query,int nParams, const onet_Oid *paramTypes);
OCresult *		OSCexecPrepared(OCconn *pConn, const char *stmtName, int nParams,
								const char * const *paramValues, const int *paramLengths, 
								const int *paramFormats, int resultFormat);
OCresult *		OSCexecParams(OCconn* pConn, const char* command, int nParams, const onet_Oid* paramTypes,
								const char* const* paramValues, const int* paramLengths, 
								const int* paramFormats, int resultFormat);
OCresult*		OSCexecParamsBatch(OCconn* pConn, const char* command, int nParams, int nBatch, const onet_Oid* paramTypes,
								const char* const* paramValues, const int* paramLengths, const int* paramFormats, int resultFormat);
OCresult*		OSCexecPreparedBatch(OCconn* pConn, const char* stmtName, int nParams, int nBatchCount, const char* const* paramValues,
								const int* paramLengths, const int* paramFormats, int resultFormat);

int				OSCmblen(const unsigned char *s, int encoding);
int				OSCdsplen(const unsigned char *s, int encoding);
void			OSCprintTuples(const OCresult *res, FILE *fout, int PrintAttNames, int TerseOutput, int colWidth);
void			OSCdisplayTuples(const OCresult *res, FILE *fp, int fillAlign,
									const char *fieldSep, int printHeader, int quiet);
void			OSCprint(FILE *fout, const OCresult *res, const OSCprintOpt *po);
OCresult *		OSCmakeEmptyOCresult(OCconn *conn, ExecStatusType status);


int				OSCbinaryTuples(const OCresult *res);
OCresult *		OSCfn(OCconn *conn, int fnid, int *result_buf, int *result_len,
				int result_is_int, ArgP_t args, int nargs);

int				OSCendcopy(OCconn *conn);
int				OSCputnbytes(OCconn *conn, const char *buffer, int nbytes);
int				OSCputline(OCconn *conn, const char *s);
int				OSCgetline(OCconn *conn, char *s, int maxlen);
int				OSCconsumeInput(OCconn *conn);
int				OSCisBusy(OCconn *conn);
OCresult *		OSCgetResult(OCconn *conn);
int				OSCsendQuery(OCconn *conn, const char *query);
OCnotify *		OSCnotifies(OCconn *conn);


int				OSCgetlength(const OCresult *result, int tup_num, int field_num);
int				OSCgetisnull(const OCresult *result, int tup_num, int field_num);
void			OSCconninfoFree(OSCconnOption *connOptions);
char *			OSCdb(const OCconn *conn);
char *			OSCuser(const OCconn *conn);
char *			OSCpass(const OCconn *conn);
char *			OSChost(const OCconn *conn);
char *			OSCport(const OCconn *conn);
char *			OSCtty(const OCconn *conn);

int				OSCLobOpen(OCLobHandle lob_handle, int mode);
int				OSCLobClose(OCLobHandle lob_handle);
int				OSCLobFree(OCLobHandle lob_handle);
int				OSCLobWrite(OCLobHandle lob_handle, OUint64_t offset, char* data, int dataLen);
int				OSCLobWriteAppend(OCLobHandle lob_handle, char* data, int datalen);
int				OSCLobRead(OCLobHandle lob_handle, OUint64_t offset, char* buf, int bufLen);
int				OSCLobTrim(OCLobHandle lob_handle, int newLen);
int				OSCLobGetLocator(OCLobHandle lob_handle, char* lob_locator, int buflen);
int				OSCLobErase(OCLobHandle lob_handle, OUint64_t offset, int eraseLen);
OUint64_t		OSCLobGetLength(OCLobHandle lob_handle);
OCLobHandle		OSCgetlob(const OCresult *res, int tup_num, int field_num);
OCLobHandle		OSCLobCreateTemp(OCconn* conn, OCLobType lobType, int cached, int duration);


OSCnoticeProcessor		OSCsetNoticeProcessor(OCconn *conn, OSCnoticeProcessor proc, void *arg);
OSCARPollingStatusType	OSCconnectPoll(OCconn *conn);
OSCARPollingStatusType  OSCresetPoll(OCconn *conn);
void					OSCreset(OCconn *conn);


OSCconnOption * OSCconndefaults(void);
int				OSCresetStart(OCconn *conn);
int				OSCrequestCancel(OCconn *conn);
char *			OSCoptions(const OCconn *conn);
int				OSCsocket(const OCconn *conn);
int				OSCbackendPID(const OCconn *conn);
int				OSCclientEncoding(const OCconn *conn);
int				OSCsetClientEncoding(OCconn *conn, const char *encoding);
char *			OSCresultErrorMessage(const OCresult *res);


#define OSCsetdb(M_OCHOST,M_OCPORT,M_OCOPT,M_OCTTY,M_DBNAME) OSCsetdbLogin(M_OCHOST,M_OCPORT,M_OCOPT,M_OCTTY,M_DBNAME,NULL,NULL,0)


#define PQnoticeProcessor	OSCnoticeProcessor
#define PQconnectStart		OSCconnectStart
#define PQconnectPoll		OSCconnectPoll
#define PQconnectdb			OSCconnectdb
#define PQsetdbLogin		OSCsetdbLogin
#define PQsetdb				OSCsetdb
#define PQfinish			OSCfinish
#define PQconndefaults		OSCconndefaults
#define PQconninfoFree		OSCconninfoFree
#define PQresetStart		OSCresetStart
#define PQresetPoll			OSCresetPoll
#define PQreset				OSCreset
#define PQrequestCancel		OSCrequestCancel
#define PQdb				OSCdb
#define PQuser				OSCuser
#define PQpass				OSCpass
#define PQhost				OSChost
#define PQport				OSCport
#define PQtty				OSCtty
#define PQoptions			OSCoptions
#define PQstatus			OSCstatus
#define PQerrorMessage		OSCerrorMessage
#define PQsocket			OSCsocket
#define PQbackendPID		OSCbackendPID
#define PQclientEncoding	OSCclientEncoding
#define PQsetClientEncoding	OSCsetClientEncoding
#define PQgetssl			OSCgetssl
#define PQtrace				OSCtrace
#define PQuntrace			OSCuntrace
#define PQsetNoticeProcessor	OSCsetNoticeProcessor
#define PQescapeString		OSCescapeString
#define PQescapeBytea		OSCescapeBytea
#define PQunescapeBytea		OSCunescapeBytea
#define PQexec				OSCexec
#define PQnotifies			OSCnotifies
#define PQfreeNotify		OSCfreeNotify
#define PQsendQuery			OSCsendQuery
#define PQgetResult			OSCgetResult
#define PQisBusy			OSCisBusy
#define PQconsumeInput		OSCconsumeInput
#define PQgetline			OSCgetline
#define PQputline			OSCputline
#define PQgetlineAsync		OSCgetlineAsync
#define PQputnbytes			OSCputnbytes
#define PQendcopy			OSCendcopy
#define PQsetnonblocking	OSCsetnonblocking
#define PQisnonblocking		OSCisnonblocking
#define PQflush				OSCflush
#define PQsendSome			OSCsendSome
#define PQresultStatus		OSCresultStatus
#define PQSQLState			OSCSQLState
#define PQresStatus			OSCresStatus
#define PQresultErrorMessage	OSCresultErrorMessage
#define PQntuples			OSCntuples
#define PQnfields			OSCnfields
#define PQbinaryTuples		OSCbinaryTuples
#define PQftype				OSCftype
#define PQfsize				OSCfsize
#define PQfmod				OSCfmod
#define PQcmdStatus			OSCcmdStatus
#define PQoidStatus			OSCoidStatus
#define PQoidValue			OSCoidValue
#define PQcmdTuples			OSCcmdTuples
#define PQgetvalue			OSCgetvalue
#define PQgetlength			OSCgetlength
#define PQgetisnull			OSCgetisnull
#define PQclear				OSCclear
#define PQmakeEmptyPGresult	OSCmakeEmptyPGresult
#define PQprint				OSCprint
#define PQdisplayTuples		OSCdisplayTuples
#define PQprintTuples		OSCprintTuples
#define PQmblen				OSCmblen
#define PQdsplen			OSCdsplen
#define PQenv2encoding		OSCenv2encoding
#define PQisReconnected		OSCisReconnected

/*!
@brief	通知信息结构
*/
struct ocNotify
{
	char	   *relname;		//!<  包含数据的关系名
	int			be_pid;			//!<  后端进程ID
};


/* 定义日期、时间、时间戳数据结构*/
typedef struct tagOSRDATE_STRUCT
{
	short int    year;
	unsigned short int   month;
	unsigned short int   day;
} DateStruct_t, *DateStructP_t;

typedef struct tagOSRTIME_STRUCT
{
	unsigned short int   hour;
	unsigned short int   minute;
	unsigned short int   second;
} TimeStruct_t, *TimeStructP_t;

typedef struct tagOSRTIMESTAMP_STRUCT
{
	short int   year;
	unsigned short int   month;
	unsigned short int   day;
	unsigned short int   hour;
	unsigned short int   minute;
	unsigned short int   second;
	unsigned short int   fraction;
} TimeStampStruct_t, *TimeStampStructP_t;

/*!
@brief	OSCprint()打印信息选项

定义OSCprint()打印信息结构:
*/
typedef char oscbool;
struct _OSCprintOpt
{
	oscbool		header;			//!< 是否打印头信息
	oscbool		align;			//!< 是否 对齐字段
	oscbool		standard;		//!< 是否是标准输出
	oscbool		html3;			//!< 是否输出html表
	oscbool		expanded;		//!< 是否扩展表
	oscbool		pager;			//!< 是否使用页
	char	   *fieldSep;		//!< 字段分隔符
	char	   *tableOpt;		//!< 插入表选项
	char	   *caption;		//!< html标题
	char	  **fieldName;		//!< 地段名数组
};

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif   /* ONET_FE_H */