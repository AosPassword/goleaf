package impls

import (
	"database/sql"
	"github.com/aospassword/leaf/core/segment/model"
	_ "github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"
)

type DefaultIDAllocDao struct {
	bd 	*sql.DB
}

var base = "root:asdasd123123@tcp(127.0.0.1:3306)/leaf?charset=utf8mb4"
var updateMaxIDStmt = "UPDATE leaf_alloc SET max_id = max_id + step WHERE biz_tag = ?"
var getLeafAllocStmt = "SELECT biz_tag, max_id, step, update_time FROM leaf_alloc WHERE biz_tag = ?"
var updateMaxIdByCustomStepStmt = "UPDATE leaf_alloc SET max_id = max_id + ? WHERE biz_tag = ?"
var getAllTagsStmt = "SELECT biz_tag FROM leaf_alloc"
var getAllLeafAllocsStmt = "SELECT biz_tag, max_id, step, update_time FROM leaf_alloc"
var DefaultIDAllocDaoBean DefaultIDAllocDao = DefaultIDAllocDao{}

func init()  {
	open(&DefaultIDAllocDaoBean)
}

func open(dao *DefaultIDAllocDao)  {
	var err error
	logrus.Info("opening DB..")
	dao.bd,err = sql.Open("mysql",base)
	if err != nil{
		panic("open db error!")
	}
}

// 获取所有的 Leaf 表中的 Alloc
func (dao *DefaultIDAllocDao)GetAllLeafAllocs() ([]model.LeafAlloc,error) {
	return getAllLeafAllocs(dao.bd)
}

// 按照数据库中的 step 更新 Leaf 对应 tag 最大的段号，并且获取新的 LeafAlloc
func (dao *DefaultIDAllocDao)UpdateMaxIdAndGetLeafAlloc(tag string) (alloc *model.LeafAlloc,err error) {
	tx,err := dao.bd.Begin()
	defer clearTransaction(tx)

	if	err != nil {
		logrus.Errorf("UpdateMaxIdAndGetLeafAlloc(tag string) \t" + tag + "\tdb.Begin()\t")
		return
	}

	err = updateMaxID(tx,tag)

	if err != nil  {
		logrus.Errorf("UpdateMaxIdAndGetLeafAlloc(tag string) \t" + tag + "\ttx.Exec(updateMaxID,tag)\t")
		return
	}

	alloc,err = getLeafAlloc(tx,tag)
	if err != nil {
		return
	}
	err = tx.Commit()
	return
}
// 按照 Custom step 更新 maxID 并且获取最新的 LeafAlloc
func (dao *DefaultIDAllocDao)UpdateMaxIdByCustomStepAndGetLeafAlloc(alloc *model.LeafAlloc) (result *model.LeafAlloc,err error) {
	tx,err := dao.bd.Begin()
	defer clearTransaction(tx)

	if	err != nil {
		logrus.Errorf("UpdateMaxIdByCustomStepAndGetLeafAlloc(alloc model.LeafAlloc) \t " + alloc.String() + "\tdb.Begin()\t")
		return
	}

	err = updateMaxIdByCustomStep(tx,alloc)
	if err != nil {
		return
	}

	result,err = getLeafAlloc(tx,alloc.Key)
	if err != nil {
		return
	}

	err = tx.Commit()
	return
}

func (dao *DefaultIDAllocDao)GetAllTags() (tags []string,err error) {
	return getAllTags(dao.bd)
}

func getAllTags(db *sql.DB) ([]string,error) {
	rows,err := db.Query(getAllTagsStmt)
	if	err != nil {
		logrus.Info("getAllTags()")
		return nil, err
	}

	return handlerStrings(rows),nil
}

// 按照 step 更新 maxID
func updateMaxID(tx *sql.Tx, tag string)  error {
	_ ,err := tx.Exec(updateMaxIDStmt,tag)

	if err != nil  {
		logrus.Errorf("updateMaxID(tx *sql.Tx, tag string) \t" + tag + "\ttx.Exec(updateMaxID,tag)\t")
		return err
	}
	return nil
}

func handlerStrings(rows *sql.Rows) []string {
	if rows != nil {
		var result = make([]string,0)
		for rows.Next() {
			var s string
			err := rows.Scan(&s)
			if err != nil{
				logrus.Error("handlerLeafAllocs(rows *sql.Rows) rows.Scan"+err.Error())
			}
			result = append(result,s)
		}
		return result
	}else {
		panic("你小子能不能消停点，这方法你别直接调啊！")
	}
	return nil
}

func handlerLeafAllocsWithTimestamp(rows *sql.Rows) []model.LeafAlloc {
	if rows != nil {
		var result = make([]model.LeafAlloc,0)
		for rows.Next() {
			alloc := model.LeafAlloc{}
			err := rows.Scan(&alloc.Key, &alloc.MaxID, &alloc.Step, &alloc.UpdateTime)
			if err != nil{
				logrus.Error("handlerLeafAllocs(rows *sql.Rows) rows.Scan"+err.Error())
			}
			result = append(result,alloc)

		}
		return result
	}else {
		panic("你小子能不能消停点，这方法你别直接调啊！")
	}
	return nil
}


func handlerLeafAllocs(rows *sql.Rows) []model.LeafAlloc {
	if rows != nil {
		var result = make([]model.LeafAlloc,0)
		for rows.Next() {
			alloc := model.LeafAlloc{}
			err := rows.Scan(&alloc.Key, &alloc.MaxID, &alloc.Step)
			if err != nil{
				logrus.Error("handlerLeafAllocs(rows *sql.Rows) rows.Scan"+err.Error())
			}
			result = append(result,alloc)

		}
		return result
	}else {
		panic("你小子能不能消停点，这方法你别直接调啊！")
	}
	return nil
}

// 获得指定 tag 的 leafAlloc
func getLeafAlloc(tx *sql.Tx,tag string) (*model.LeafAlloc,error) {
	rows,err := tx.Query(getLeafAllocStmt,tag)
	if	err != nil {
		logrus.Errorf("UpdateMaxIdAndGetLeafAlloc(tag string) \t" + tag + "\ttx.Query(getLeafAlloc,tag)\t")
		return &model.LeafAlloc{}, err
	}
	return &handlerLeafAllocs(rows)[0],nil
}
// 获取全部的 leafAlloc
func getAllLeafAllocs(db *sql.DB) ([]model.LeafAlloc,error) {
	rows,err := db.Query(getAllLeafAllocsStmt)
	if	err!= nil{
		logrus.Error("GetAllLeafAllocs() db.query:"+err.Error())
		return nil,err
	}

	return handlerLeafAllocsWithTimestamp(rows),nil
}
// 按照 CustomStep 提交
func updateMaxIdByCustomStep(tx *sql.Tx ,alloc *model.LeafAlloc) error {
	 _,err := tx.Exec(updateMaxIdByCustomStepStmt,alloc.Step,alloc.Key)
	if err != nil {
		logrus.Errorf("updateMaxIdByCustomStep(tx *sql.Tx ,alloc model.LeafAlloc)" + alloc.String())
		return err
	}
	return nil
}

// close tx
func clearTransaction(tx *sql.Tx){
	err := tx.Rollback()


	if err != sql.ErrTxDone && err != nil{
		logrus.Errorf("clearTransaction(tx *sql.Tx)" + err.Error())
	}
}
