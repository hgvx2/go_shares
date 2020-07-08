package db_opt

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"time"
)

const (
	DB_TYPE_MYSQL = iota
	DB_TYPE_SQLITE
	DB_TYPE_ORICAL
	DB_TYPE_SQLSERVER
)

type dbOpt struct {
	m_nDBType int         // 数据库类型
	m_pDB     interface{} // 数据库连接指针
	m_strDSN  string

	m_strTable string // 表名
	m_bConnect bool
}

type tableOpt struct {
	dbOpt
	m_pDBOpt *sql.DB
	m_strSql string // 最后执行的SQL

	m_strField string //  查询的field 字段
	m_strWhere string
	m_strLimit string
}

func (this *dbOpt) GetTable(strTable string) (Tabler, error) {
	ret, err := Tabler(nil), error(nil)
	for {
		if !this.m_bConnect {
			err = fmt.Errorf("连接已关闭")
			break
		}
		var pDBOpt *sql.DB
		var bOk bool
		if pDBOpt, bOk = this.m_pDB.(*sql.DB); bOk {
			ret = &tableOpt{*this, pDBOpt, "", "", "", ""}
		} else {
			err = fmt.Errorf("断言失败")
		}
		break
	}
	return ret, err
}
func (this *dbOpt) Close() {
	if this.m_bConnect {
		this.m_bConnect = false
	} else {
		return
	}
	if pDb, ok := this.m_pDB.(*sql.DB); ok {
		pDb.Close()
		delete(g_mapDB, this.m_strDSN)
	}
}

func (this *tableOpt) Exec(strSql string, args ...interface{}) error {
	this.m_strSql = fmt.Sprintf(strSql, args...)
	_, err := this.m_pDBOpt.Exec(this.m_strSql)
	return err
}

func (this *tableOpt) Query(strSql string, args ...interface{}) ([]map[string]interface{}, error) {
	pRet, err := []map[string]interface{}(nil), error(nil)
	for {
		var rows *sql.Rows
		this.m_strSql = fmt.Sprintf(strSql, args...)
		rows, err = this.m_pDBOpt.Query(this.m_strSql)
		if nil != err {
			break
		}
		defer rows.Close()
		var arrColumns []string
		arrColumns, err = rows.Columns()
		if nil != err {
			break
		}
		lenColumn := len(arrColumns)
		if 0 >= lenColumn {
			err = fmt.Errorf("获取的列数为0")
			break
		}
		pRet = make([]map[string]interface{}, 0)
		item := make(map[string]interface{})
		arrRecv := make([]interface{}, lenColumn)
		for rows.Next() {
			if err = rows.Scan(arrRecv...); nil != err {
				break
			}
			for i, v := range arrColumns {
				item[v] = arrRecv[i]
			}
			pRet = append(pRet, item)
		}
		break
	}
	return pRet, err
}

func (this *tableOpt) Add(arrMap []map[string]interface{}) error {
	err := error(nil)
	for {
		arrLen := len(arrMap)
		if 0 >= arrLen {
			err = fmt.Errorf("参数错误, 长度为0")
			break
		}
		arrColumNume := make([]string, 0)
		for k, _ := range arrMap[0] {
			arrColumNume = append(arrColumNume, k)
		}
		this.m_strSql = "INSERT INTO " + this.m_strTable
		for i, v := range arrColumNume {
			if 0 == i {
				this.m_strSql += " (" + v
				continue
			}
			this.m_strSql += " ," + v
		}
		this.m_strSql += ") VALUES"

		for i, m := range arrMap {
			if 0 == i {
				this.m_strSql += " ("
			} else {
				this.m_strSql += " ,("
			}
			for _, v := range m {
				if _, ok := v.(string); ok {
					this.m_strSql += fmt.Sprintf("'%v'", v)
					continue
				}
				this.m_strSql += fmt.Sprintf("%v", v)
			}
			this.m_strSql += ")"
		}
		_, err = this.m_pDBOpt.Exec(this.m_strSql)
		break
	}
	return err
}

func (this *tableOpt) Select() ([]map[string]interface{}, error) {
	pRet, err := []map[string]interface{}(nil), error(nil)
	for {
		this.m_strSql = "select "
		if 0 <= len(this.m_strField) {
			this.m_strField = "*"
		}
		this.m_strSql += this.m_strField + " from " + this.m_strTable
		if 0 < len(this.m_strWhere) {
			this.m_strSql += " where " + this.m_strWhere
		}
		if 0 < len(this.m_strLimit) {
			this.m_strSql += " " + this.m_strLimit
		}
		pRet, err = this.Query(this.m_strSql)
		break
	}
	return pRet, err
}

func (this *tableOpt) Where(strWhere string, args ...interface{}) Wherer {
	this.m_strWhere = fmt.Sprintf(strWhere, args...)
	return this
}

func (this *tableOpt) Field(strField string, args ...interface{}) Fielder {
	this.m_strField = fmt.Sprintf(strField, args...)
	return this
}

// r1:totalPage  r2:totalNums
func (this *tableOpt) Page(nPageSize, nPageIndex int) (int, int, Pager, error) {
	nTotalPage, nTotalNums, page, err := 0, 0, Pager(nil), error(nil)
	for {
		if 0 >= nPageSize || 0 >= nPageIndex {
			err = fmt.Errorf("参数错误")
			break
		}

		if nTotalNums, err = this.Count(); nil != err {
			break
		}
		if 0 == nTotalNums {
			page = this
			break
		}

		if 0 == nTotalNums%nPageSize {
			nTotalPage = nTotalNums / nPageSize
		} else {
			nTotalPage = nTotalNums/nPageSize + 1
		}
		nStartIndex := (nPageIndex - 1) * nPageSize
		this.m_strLimit = fmt.Sprintf("limit %d, %d", nStartIndex, nPageSize)
		page = this
		break
	}
	return nTotalPage, nTotalNums, page, err
}

func (this *tableOpt) Count() (int, error) {
	nRet, err := 0, error(nil)
	for {
		this.m_strSql = "select count(*) as total from " + this.m_strTable
		if 0 < len(this.m_strWhere) {
			this.m_strSql += " " + this.m_strWhere
		}
		var arrMap []map[string]interface{}
		if arrMap, err = this.Query(this.m_strSql); nil != err {
			break
		}
		if 1 != len(arrMap) {
			err = fmt.Errorf("查询数量出错")
			break
		}
		var bOk bool
		if nRet, bOk = arrMap[0]["total"].(int); !bOk {
			err = fmt.Errorf("断言错误")
		}
		break
	}
	return nRet, err
}

func (this *tableOpt) Update(mapDate map[string]interface{}) error {
	err := error(nil)
	for {
		if 0 >= len(mapDate) {
			err = fmt.Errorf("参数为空")
			break
		}
		this.m_strSql = "update " + this.m_strTable + " set "
		bFirst := true
		for k, v := range mapDate {
			if bFirst {
				bFirst = false
			} else {
				this.m_strSql += ","
			}
			if _, ok := v.(string); ok {
				this.m_strSql += fmt.Sprintf("%s='%v'", k, v)
				continue
			}
			this.m_strSql += fmt.Sprintf("%s=%v", k, v)
		}
		this.m_strSql += " where " + this.m_strWhere
		err = this.Exec(this.m_strSql)
		break
	}
	return err
}
func (this *tableOpt) Delete() error {
	err := error(nil)
	for {
		this.m_strSql = "delete from " + this.m_strTable + " where " + this.m_strWhere
		err = this.Exec(this.m_strSql)
		break
	}
	return err
}

type DBOpter interface {
	GetTable(strTable string) (Tabler, error)
	Close()
}

type Tabler interface {
	Exec(string, ...interface{}) error
	Query(string, ...interface{}) ([]map[string]interface{}, error)

	Add([]map[string]interface{}) error

	Select() ([]map[string]interface{}, error)
	Where(string, ...interface{}) Wherer
	Field(string, ...interface{}) Fielder
	// r1:totalPage  r2:totalNums
	Page(nPageSize, nPageIndex int) (int, int, Pager, error)
	Count() (int, error)
}

type Wherer interface {
	Update(map[string]interface{}) error
	Delete() error

	Select() ([]map[string]interface{}, error)
	Field(string, ...interface{}) Fielder
	// r1:totalPage  r2:totalNums
	Page(nPageSize, nPageIndex int) (int, int, Pager, error)
	Count() (int, error)
}

type Fielder interface {
	Select() ([]map[string]interface{}, error)
	// r1:totalPage  r2:totalNums
	Page(nPageSize, nPageIndex int) (int, int, Pager, error)
}

type Pager interface {
	Select() ([]map[string]interface{}, error)
}

var g_mapDB map[string]*dbOpt

func InitDBOpt(strUser, strPass, strHost, strPort, strDB, strCharset string, nType int) (DBOpter, error) {
	pRet, err := DBOpter(nil), error(nil)
	for {
		if nil == g_mapDB {
			g_mapDB = make(map[string]*dbOpt)
		}
		switch nType {
		case DB_TYPE_MYSQL:
			pRet, err = connectMysql(strUser, strPass, strHost, strPort, strDB, strCharset)
		case DB_TYPE_ORICAL:
			err = fmt.Errorf("暂未实现 orical数据库的操作")
		case DB_TYPE_SQLITE:
			err = fmt.Errorf("暂未实现 sqllite数据库的操作")
		case DB_TYPE_SQLSERVER:
			err = fmt.Errorf("暂未实现 sqlserver数据库的操作")
		default:
			err = fmt.Errorf("未知的类型", nType)
		}
		break
	}
	return pRet, err
}

// 连接mysql
func connectMysql(strUser, strPass, strHost, strPort, strDB, strCharset string) (*dbOpt, error) {
	pRet, err := (*dbOpt)(nil), error(nil)
	for {
		strDbDns := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=%s",
			strUser,
			strPass,
			strHost,
			strPort,
			strDB,
			strCharset)

		var bFind bool
		if pRet, bFind = g_mapDB[strDbDns]; bFind && pRet.m_bConnect {
			break
		}

		var pDB *sql.DB
		pDB, err = sql.Open("mysql", strDbDns)
		if nil != err {
			err = fmt.Errorf("open db err, " + err.Error())
			break
		}
		// 最大连接数
		pDB.SetMaxOpenConns(50)
		// 闲置连接数
		pDB.SetMaxIdleConns(10)
		// 最大连接周期
		pDB.SetConnMaxLifetime(100 * time.Second)
		if err = pDB.Ping(); nil != err {
			panic("ping error: " + err.Error())
		}
		g_mapDB[strDbDns] = &dbOpt{DB_TYPE_MYSQL, pDB, strDbDns, "", true}
		break
	}
	return pRet, err
}
