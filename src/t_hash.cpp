#include "t_hash.h"
#include "db_impl.h"
#include "leveldb/write_batch.h"

namespace ssdb{

static int hset_one(DbImpl *ssdb, const Bytes &name, const Bytes &key, const Bytes &val);
static int hdel_one(DbImpl *ssdb, const Bytes &name, const Bytes &key);
static int incr_hsize(DbImpl *ssdb, const Bytes &name, int64_t incr);

/**
 * @return -1: error, 0: item updated, 1: new item inserted
 */
int DbImpl::hset(const Bytes &name, const Bytes &key, const Bytes &val){
	Transaction trans(writer);

	int ret = hset_one(this, name, key, val);
	if(ret >= 0){
		if(ret > 0){
			if(incr_hsize(this, name, ret) == -1){
				return -1;
			}
		}
		leveldb::Status s = writer->commit();
		if(!s.ok()){
			return -1;
		}
	}
	return ret;
}

int DbImpl::hdel(const Bytes &name, const Bytes &key){
	Transaction trans(writer);

	int ret = hdel_one(this, name, key);
	if(ret >= 0){
		if(ret > 0){
			if(incr_hsize(this, name, -ret) == -1){
				return -1;
			}
		}
		leveldb::Status s = writer->commit();
		if(!s.ok()){
			return -1;
		}
	}
	return ret;
}

int DbImpl::hincr(const Bytes &name, const Bytes &key, int64_t by, std::string *new_val){
	Transaction trans(writer);

	int64_t val;
	std::string old;
	int ret = this->hget(name, key, &old);
	if(ret == -1){
		return -1;
	}else if(ret == 0){
		val = by;
	}else{
		val = str_to_int64(old.data(), old.size()) + by;
	}

	*new_val = int64_to_str(val);
	ret = hset_one(this, name, key, *new_val);
	if(ret >= 0){
		if(ret > 0){
			if(incr_hsize(this, name, ret) == -1){
				return -1;
			}
		}
		leveldb::Status s = writer->commit();
		if(!s.ok()){
			return -1;
		}
	}
	return ret;
}

int64_t DbImpl::hsize(const Bytes &name){
	std::string size_key = encode_hsize_key(name);
	std::string val;
	leveldb::Status s;

	s = db->Get(leveldb::ReadOptions(), size_key, &val);
	if(s.IsNotFound()){
		return 0;
	}else if(!s.ok()){
		return -1;
	}else{
		if(val.size() != sizeof(uint64_t)){
			return 0;
		}
		int64_t ret = *(int64_t *)val.data();
		return ret < 0? 0 : ret;
	}
}

int DbImpl::hget(const Bytes &name, const Bytes &key, std::string *val){
	std::string dbkey = encode_hash_key(name, key);
	leveldb::Status s = db->Get(leveldb::ReadOptions(), dbkey, val);
	if(s.IsNotFound()){
		return 0;
	}
	if(!s.ok()){
		return -1;
	}
	return 1;
}

HIterator* DbImpl::hscan(const Bytes &name, const Bytes &start, const Bytes &end, uint64_t limit){
	std::string key_start, key_end;

	key_start = encode_hash_key(name, start);
	if(!end.empty()){
		key_end = encode_hash_key(name, end);
	}
	//dump(key_start.data(), key_start.size(), "scan.start");
	//dump(key_end.data(), key_end.size(), "scan.end");

	return new HIterator(this->iterator(key_start, key_end, limit), name);
}

HIterator* DbImpl::hrscan(const Bytes &name, const Bytes &start, const Bytes &end, uint64_t limit){
	std::string key_start, key_end;

	key_start = encode_hash_key(name, start);
	if(start.empty()){
		key_start.append(1, 255);
	}
	if(!end.empty()){
		key_end = encode_hash_key(name, end);
	}
	//dump(key_start.data(), key_start.size(), "scan.start");
	//dump(key_end.data(), key_end.size(), "scan.end");

	return new HIterator(this->rev_iterator(key_start, key_end, limit), name);
}

int DbImpl::hlist(const Bytes &name_s, const Bytes &name_e, uint64_t limit,
		std::vector<std::string> *list){
	std::string start;
	std::string end;
	start = encode_hsize_key(name_s);
	if(!name_e.empty()){
		end = encode_hsize_key(name_e);
	}
	Iterator *it = this->iterator(start, end, limit);
	while(it->next()){
		Bytes ks = it->key();
		if(ks.data()[0] != DataType::HSIZE){
			break;
		}
		std::string n;
		if(decode_hsize_key(ks, &n) == -1){
			continue;
		}
		list->push_back(n);
	}
	delete it;
	return 0;
}

// returns the number of newly added items
static int hset_one(DbImpl *ssdb, const Bytes &name, const Bytes &key, const Bytes &val){
	if(name.empty() || key.empty()){
		log_error("empty name or key!");
		return -1;
	}
	if(name.size() > SSDB_KEY_LEN_MAX ){
		log_error("name too long! %s", hexmem(name.data(), name.size()).c_str());
		return -1;
	}
	if(key.size() > SSDB_KEY_LEN_MAX){
		log_error("key too long! %s", hexmem(key.data(), key.size()).c_str());
		return -1;
	}
	int ret = 0;
	std::string dbval;
	if(ssdb->hget(name, key, &dbval) == 0){ // not found
		std::string hkey = encode_hash_key(name, key);
		ssdb->writer->Put(hkey, val);
		ret = 1;
	}else{
		if(dbval != val){
			std::string hkey = encode_hash_key(name, key);
			ssdb->writer->Put(hkey, val);
		}
		ret = 0;
	}
	return ret;
}

static int hdel_one(DbImpl *ssdb, const Bytes &name, const Bytes &key){
	if(name.size() > SSDB_KEY_LEN_MAX ){
		log_error("name too long! %s", hexmem(name.data(), name.size()).c_str());
		return -1;
	}
	if(key.size() > SSDB_KEY_LEN_MAX){
		log_error("key too long! %s", hexmem(key.data(), key.size()).c_str());
		return -1;
	}
	std::string dbval;
	if(ssdb->hget(name, key, &dbval) == 0){
		return -1;
	}

	std::string hkey = encode_hash_key(name, key);
	ssdb->writer->Delete(hkey);
	
	return 1;
}

static int incr_hsize(DbImpl *ssdb, const Bytes &name, int64_t incr){
	int64_t size = ssdb->hsize(name);
	size += incr;
	std::string size_key = encode_hsize_key(name);
	if(size == 0){
		ssdb->writer->Delete(size_key);
	}else{
		ssdb->writer->Put(size_key, Bytes((char *)&size, sizeof(int64_t)));
	}
	return 0;
}

}; // end namespace ssdb
