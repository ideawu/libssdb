#include "writer.h"
#include "util/log.h"
#include "util/strings.h"
#include <map>

namespace ssdb{

Writer::Writer(leveldb::DB *db){
	this->db = db;
}

Writer::~Writer(){
}

void Writer::begin(){
	batch.Clear();
}

void Writer::rollback(){
	//
}

leveldb::Status Writer::commit(){
	leveldb::WriteOptions write_opts;
	leveldb::Status s = db->Write(write_opts, &batch);
	return s;
}

// leveldb put
void Writer::Put(const Bytes &key, const Bytes &val){
	batch.Put(leveldb::Slice(key.data(), key.size()), leveldb::Slice(val.data(), val.size()));
}

// leveldb delete
void Writer::Delete(const Bytes &key){
	batch.Delete(leveldb::Slice(key.data(), key.size()));
}

}; // end namespace ssdb

