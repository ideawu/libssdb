#include "t_kv.h"
#include "db_impl.h"
#include "leveldb/write_batch.h"

namespace ssdb{

int DbImpl::multi_set(const std::vector<Bytes> &kvs, int offset){
	Transaction trans(writer);

	std::vector<Bytes>::const_iterator it;
	it = kvs.begin() + offset;
	for(; it != kvs.end(); it += 2){
		const Bytes &key = *it;
		if(key.empty()){
			log_error("empty key!");
			return 0;
			//return -1;
		}
		const Bytes &val = *(it + 1);
		std::string buf = encode_kv_key(key);
		writer->Put(buf, val);
	}
	leveldb::Status s = writer->commit();
	if(!s.ok()){
		log_error("multi_set error: %s", s.ToString().c_str());
		return -1;
	}
	return (kvs.size() - offset)/2;
}

int DbImpl::multi_del(const std::vector<Bytes> &keys, int offset){
	Transaction trans(writer);

	std::vector<Bytes>::const_iterator it;
	it = keys.begin() + offset;
	for(; it != keys.end(); it++){
		const Bytes &key = *it;
		std::string buf = encode_kv_key(key);
		writer->Delete(buf);
	}
	leveldb::Status s = writer->commit();
	if(!s.ok()){
		log_error("multi_del error: %s", s.ToString().c_str());
		return -1;
	}
	return keys.size() - offset;
}

int DbImpl::set(const Bytes &key, const Bytes &val){
	if(key.empty()){
		log_error("empty key!");
		//return -1;
		return 0;
	}
	Transaction trans(writer);

	std::string buf = encode_kv_key(key);
	writer->Put(buf, val);
	leveldb::Status s = writer->commit();
	if(!s.ok()){
		log_error("set error: %s", s.ToString().c_str());
		return -1;
	}
	return 1;
}

int DbImpl::del(const Bytes &key){
	Transaction trans(writer);

	std::string buf = encode_kv_key(key);
	writer->begin();
	writer->Delete(buf);
	leveldb::Status s = writer->commit();
	if(!s.ok()){
		log_error("del error: %s", s.ToString().c_str());
		return -1;
	}
	return 1;
}

int DbImpl::incr(const Bytes &key, int64_t by, std::string *new_val){
	Transaction trans(writer);

	int64_t val;
	std::string old;
	int ret = this->get(key, &old);
	if(ret == -1){
		return -1;
	}else if(ret == 0){
		val = by;
	}else{
		val = str_to_int64(old.data(), old.size()) + by;
	}

	*new_val = int64_to_str(val);
	std::string buf = encode_kv_key(key);
	
	writer->Put(buf, *new_val);

	leveldb::Status s = writer->commit();
	if(!s.ok()){
		log_error("del error: %s", s.ToString().c_str());
		return -1;
	}
	return 1;
}

int DbImpl::get(const Bytes &key, std::string *val){
	std::string buf = encode_kv_key(key);

	leveldb::Status s = db->Get(leveldb::ReadOptions(), buf, val);
	if(s.IsNotFound()){
		return 0;
	}
	if(!s.ok()){
		log_error("get error: %s", s.ToString().c_str());
		return -1;
	}
	return 1;
}

KIterator* DbImpl::scan(const Bytes &start, const Bytes &end, uint64_t limit){
	std::string key_start, key_end;
	key_start = encode_kv_key(start);
	if(end.empty()){
		key_end = "";
	}else{
		key_end = encode_kv_key(end);
	}
	//dump(key_start.data(), key_start.size(), "scan.start");
	//dump(key_end.data(), key_end.size(), "scan.end");

	return new KIterator(this->iterator(key_start, key_end, limit));
}

KIterator* DbImpl::rscan(const Bytes &start, const Bytes &end, uint64_t limit){
	std::string key_start, key_end;

	key_start = encode_kv_key(start);
	if(start.empty()){
		key_start.append(1, 255);
	}
	if(end.empty()){
		key_end = "";
	}else{
		key_end = encode_kv_key(end);
	}
	//dump(key_start.data(), key_start.size(), "scan.start");
	//dump(key_end.data(), key_end.size(), "scan.end");

	return new KIterator(this->rev_iterator(key_start, key_end, limit));
}


}; // end namespace ssdb
