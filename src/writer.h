#ifndef SSDB_WRITER_H_
#define SSDB_WRITER_H_

#include <string>
#include "include.h"
#include "leveldb/db.h"
#include "leveldb/options.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"
#include "leveldb/write_batch.h"
#include "util/thread.h"
#include "ssdb/bytes.h"


namespace ssdb{

// circular queue
class Writer{
	private:
		leveldb::DB *db;
		leveldb::WriteBatch batch;
	public:
		Mutex mutex;

		Writer(leveldb::DB *db);
		~Writer();
		
		void begin();
		void rollback();
		leveldb::Status commit();
		// leveldb put
		//void Put(const leveldb::Slice& key, const leveldb::Slice& value);
		void Put(const Bytes &key, const Bytes &val);
		// leveldb delete
		void Delete(const Bytes &key);
};

class Transaction{
private:
	Writer *logs;
public:
	Transaction(Writer *logs){
		this->logs = logs;
		logs->mutex.lock();
		logs->begin();
	}
	
	~Transaction(){
		// it is safe to call rollback after commit
		logs->rollback();
		logs->mutex.unlock();
	}
};


}; // end namespace ssdb

#endif
