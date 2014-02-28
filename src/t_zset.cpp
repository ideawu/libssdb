#include <limits.h>
#include "t_zset.h"
#include "db_impl.h"
#include "leveldb/write_batch.h"

namespace ssdb{

static const char *SSDB_SCORE_MIN		= "-9223372036854775808";
static const char *SSDB_SCORE_MAX		= "+9223372036854775807";

static int zset_one(DbImpl *ssdb, const Bytes &name, const Bytes &key, const Bytes &score);
static int zdel_one(DbImpl *ssdb, const Bytes &name, const Bytes &key);
static int incr_zsize(DbImpl *ssdb, const Bytes &name, int64_t incr);

/**
 * @return -1: error, 0: item updated, 1: new item inserted
 */
int DbImpl::zset(const Bytes &name, const Bytes &key, const Bytes &score){
	Transaction trans(writer);

	int ret = zset_one(this, name, key, score);
	if(ret >= 0){
		if(ret > 0){
			if(incr_zsize(this, name, ret) == -1){
				return -1;
			}
		}
		leveldb::Status s = writer->commit();
		if(!s.ok()){
			log_error("zset error: %s", s.ToString().c_str());
			return -1;
		}
	}
	return ret;
}

int DbImpl::zdel(const Bytes &name, const Bytes &key){
	Transaction trans(writer);

	int ret = zdel_one(this, name, key);
	if(ret >= 0){
		if(ret > 0){
			if(incr_zsize(this, name, -ret) == -1){
				return -1;
			}
		}
		leveldb::Status s = writer->commit();
		if(!s.ok()){
			log_error("zdel error: %s", s.ToString().c_str());
			return -1;
		}
	}
	return ret;
}

int DbImpl::zincr(const Bytes &name, const Bytes &key, int64_t by, std::string *new_val){
	Transaction trans(writer);

	int64_t val;
	std::string old;
	int ret = this->zget(name, key, &old);
	if(ret == -1){
		return -1;
	}else if(ret == 0){
		val = by;
	}else{
		val = str_to_int64(old.data(), old.size()) + by;
	}

	*new_val = int64_to_str(val);

	ret = zset_one(this, name, key, *new_val);
	if(ret >= 0){
		if(ret > 0){
			if(incr_zsize(this, name, ret) == -1){
				return -1;
			}
		}
		leveldb::Status s = writer->commit();
		if(!s.ok()){
			log_error("zset error: %s", s.ToString().c_str());
			return -1;
		}
	}
	return ret;
}

int64_t DbImpl::zsize(const Bytes &name){
	std::string size_key = encode_zsize_key(name);
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

int DbImpl::zget(const Bytes &name, const Bytes &key, std::string *score){
	std::string buf = encode_zset_key(name, key);
	leveldb::Status s = db->Get(leveldb::ReadOptions(), buf, score);
	if(s.IsNotFound()){
		return 0;
	}
	if(!s.ok()){
		log_error("zget error: %s", s.ToString().c_str());
		return -1;
	}
	return 1;
}

static ZIterator* ziterator(
	DbImpl *ssdb,
	const Bytes &name, const Bytes &key_start,
	const Bytes &score_start, const Bytes &score_end,
	uint64_t limit, Iterator::Direction direction)
{
	if(direction == Iterator::FORWARD){
		std::string start, end;
		if(score_start.empty()){
			start = encode_zscore_key(name, key_start, SSDB_SCORE_MIN);
		}else{
			start = encode_zscore_key(name, key_start, score_start);
		}
		if(score_end.empty()){
			end = encode_zscore_key(name, "\xff", SSDB_SCORE_MAX);
		}else{
			end = encode_zscore_key(name, "\xff", score_end);
		}
		return new ZIterator(ssdb->iterator(start, end, limit), name);
	}else{
		std::string start, end;
		if(score_start.empty()){
			start = encode_zscore_key(name, key_start, SSDB_SCORE_MAX);
		}else{
			start = encode_zscore_key(name, key_start, score_start);
		}
		if(score_end.empty()){
			end = encode_zscore_key(name, "", SSDB_SCORE_MIN);
		}else{
			end = encode_zscore_key(name, "", score_end);
		}
		return new ZIterator(ssdb->rev_iterator(start, end, limit), name);
	}
}

int64_t DbImpl::zrank(const Bytes &name, const Bytes &key){
	ZIterator *it = ziterator(this, name, "", "", "", INT_MAX, Iterator::FORWARD);
	uint64_t ret = 0;
	while(true){
		if(it->next() == false){
			ret = -1;
			break;
		}
		if(key == it->key){
			break;
		}
		ret ++;
	}
	delete it;
	return ret;
}

int64_t DbImpl::zrrank(const Bytes &name, const Bytes &key){
	ZIterator *it = ziterator(this, name, "", "", "", INT_MAX, Iterator::BACKWARD);
	uint64_t ret = 0;
	while(true){
		if(it->next() == false){
			ret = -1;
			break;
		}
		if(key == it->key){
			break;
		}
		ret ++;
	}
	delete it;
	return ret;
}

ZIterator* DbImpl::zrange(const Bytes &name, uint64_t offset, uint64_t limit){
	if(offset + limit > limit){
		limit = offset + limit;
	}
	ZIterator *it = ziterator(this, name, "", "", "", limit, Iterator::FORWARD);
	it->skip(offset);
	return it;
}

ZIterator* DbImpl::zrrange(const Bytes &name, uint64_t offset, uint64_t limit){
	if(offset + limit > limit){
		limit = offset + limit;
	}
	ZIterator *it = ziterator(this, name, "", "", "", limit, Iterator::BACKWARD);
	it->skip(offset);
	return it;
}

ZIterator* DbImpl::zscan(const Bytes &name, const Bytes &key,
		const Bytes &score_start, const Bytes &score_end, uint64_t limit)
{
	std::string score;
	// if only key is specified, load its value
	if(!key.empty() && score_start.empty()){
		this->zget(name, key, &score);
	}else{
		score = score_start.String();
	}
	return ziterator(this, name, key, score, score_end, limit, Iterator::FORWARD);
}

ZIterator* DbImpl::zrscan(const Bytes &name, const Bytes &key,
		const Bytes &score_start, const Bytes &score_end, uint64_t limit)
{
	std::string score;
	// if only key is specified, load its value
	if(!key.empty() && score_start.empty()){
		this->zget(name, key, &score);
	}else{
		score = score_start.String();
	}
	return ziterator(this, name, key, score, score_end, limit, Iterator::BACKWARD);
}

int DbImpl::zlist(const Bytes &name_s, const Bytes &name_e, uint64_t limit,
		std::vector<std::string> *list){
	std::string start;
	std::string end;
	start = encode_zsize_key(name_s);
	if(!name_e.empty()){
		end = encode_zsize_key(name_e);
	}
	Iterator *it = this->iterator(start, end, limit);
	while(it->next()){
		Bytes ks = it->key();
		//dump(ks.data(), ks.size());
		if(ks.data()[0] != DataType::ZSIZE){
			break;
		}
		std::string n;
		if(decode_zsize_key(ks, &n) == -1){
			continue;
		}
		list->push_back(n);
	}
	delete it;
	return 0;
}

static std::string filter_score(const Bytes &score){
	int64_t s = score.Int64();
	char buf[32];
	snprintf(buf, sizeof(buf), "%" PRId64 "", s);
	return std::string(buf);
}

// returns the number of newly added items
static int zset_one(DbImpl *ssdb, const Bytes &name, const Bytes &key, const Bytes &score){
	if(name.empty() || key.empty()){
		log_error("empty name or key!");
		return 0;
		//return -1;
	}
	if(name.size() > SSDB_KEY_LEN_MAX ){
		log_error("name too long!");
		return -1;
	}
	if(key.size() > SSDB_KEY_LEN_MAX){
		log_error("key too long!");
		return -1;
	}
	std::string new_score = filter_score(score);
	std::string old_score;
	int found = ssdb->zget(name, key, &old_score);
	if(found == 0 || old_score != new_score){
		std::string k0, k1, k2;

		if(found){
			// delete zscore key
			k1 = encode_zscore_key(name, key, old_score);
			ssdb->writer->Delete(k1);
		}

		// add zscore key
		k2 = encode_zscore_key(name, key, new_score);
		ssdb->writer->Put(k2, "");

		// update zset
		k0 = encode_zset_key(name, key);
		ssdb->writer->Put(k0, new_score);

		return found? 0 : 1;
	}
	return 0;
}

static int zdel_one(DbImpl *ssdb, const Bytes &name, const Bytes &key){
	if(name.size() > SSDB_KEY_LEN_MAX ){
		log_error("name too long!");
		return -1;
	}
	if(key.size() > SSDB_KEY_LEN_MAX){
		log_error("key too long!");
		return -1;
	}
	std::string old_score;
	int found = ssdb->zget(name, key, &old_score);
	if(found != 1){
		return 0;
	}

	std::string k0, k1;
	// delete zscore key
	k1 = encode_zscore_key(name, key, old_score);
	ssdb->writer->Delete(k1);

	// delete zset
	k0 = encode_zset_key(name, key);
	ssdb->writer->Delete(k0);

	return 1;
}

static int incr_zsize(DbImpl *ssdb, const Bytes &name, int64_t incr){
	int64_t size = ssdb->zsize(name);
	size += incr;
	std::string size_key = encode_zsize_key(name);
	if(size == 0){
		ssdb->writer->Delete(size_key);
	}else{
		ssdb->writer->Put(size_key, Bytes((char *)&size, sizeof(int64_t)));
	}
	return 0;
}

}; // end namespace ssdb
