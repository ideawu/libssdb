#ifndef DbImpl_IMPL_H_
#define DbImpl_IMPL_H_

#include <vector>
#include "include.h"
#include "leveldb/db.h"
#include "leveldb/options.h"

#include "ssdb/ssdb.h"
#include "util/log.h"
#include "writer.h"

namespace ssdb{

class DbImpl : public Db{
public:
	leveldb::DB* db;
	leveldb::Options options;
	Writer *writer;
	
	DbImpl();
	virtual ~DbImpl();

	// return (start, end], not include start
	virtual Iterator* iterator(const std::string &start, const std::string &end, uint64_t limit);
	virtual Iterator* rev_iterator(const std::string &start, const std::string &end, uint64_t limit);

	//void flushdb();
	virtual std::vector<std::string> info();
	virtual void compact();
	virtual int key_range(std::vector<std::string> *keys);

	/* raw operates */

	// repl: whether to sync this operation to slaves
	virtual int raw_set(const Bytes &key, const Bytes &val);
	virtual int raw_del(const Bytes &key);
	virtual int raw_get(const Bytes &key, std::string *val);

	/* key value */

	virtual int set(const Bytes &key, const Bytes &val);
	virtual int del(const Bytes &key);
	virtual int incr(const Bytes &key, int64_t by, std::string *new_val);
	virtual int multi_set(const std::vector<Bytes> &kvs, int offset=0);
	virtual int multi_del(const std::vector<Bytes> &keys, int offset=0);
	
	virtual int get(const Bytes &key, std::string *val);
	// return (start, end]
	virtual KIterator* scan(const Bytes &start, const Bytes &end, uint64_t limit);
	virtual KIterator* rscan(const Bytes &start, const Bytes &end, uint64_t limit);

	/* hash */

	virtual int hset(const Bytes &name, const Bytes &key, const Bytes &val);
	virtual int hdel(const Bytes &name, const Bytes &key);
	virtual int hincr(const Bytes &name, const Bytes &key, int64_t by, std::string *new_val);
	//int multi_hset(const Bytes &name, const std::vector<Bytes> &kvs, int offset=0);
	//int multi_hdel(const Bytes &name, const std::vector<Bytes> &keys, int offset=0);

	virtual int64_t hsize(const Bytes &name);
	virtual int hget(const Bytes &name, const Bytes &key, std::string *val);
	virtual int hlist(const Bytes &name_s, const Bytes &name_e, uint64_t limit,
			std::vector<std::string> *list);
	virtual HIterator* hscan(const Bytes &name, const Bytes &start, const Bytes &end, uint64_t limit);
	virtual HIterator* hrscan(const Bytes &name, const Bytes &start, const Bytes &end, uint64_t limit);

	/* zset */

	virtual int zset(const Bytes &name, const Bytes &key, const Bytes &score);
	virtual int zdel(const Bytes &name, const Bytes &key);
	virtual int zincr(const Bytes &name, const Bytes &key, int64_t by, std::string *new_val);
	//int multi_zset(const Bytes &name, const std::vector<Bytes> &kvs, int offset=0);
	//int multi_zdel(const Bytes &name, const std::vector<Bytes> &keys, int offset=0);
	
	virtual int64_t zsize(const Bytes &name);
	/**
	 * @return -1: error; 0: not found; 1: found
	 */
	virtual int zget(const Bytes &name, const Bytes &key, std::string *score);
	virtual int64_t zrank(const Bytes &name, const Bytes &key);
	virtual int64_t zrrank(const Bytes &name, const Bytes &key);
	virtual ZIterator* zrange(const Bytes &name, uint64_t offset, uint64_t limit);
	virtual ZIterator* zrrange(const Bytes &name, uint64_t offset, uint64_t limit);
	/**
	 * scan by score, but won't return @key if key.score=score_start.
	 * return (score_start, score_end]
	 */
	virtual ZIterator* zscan(const Bytes &name, const Bytes &key,
			const Bytes &score_start, const Bytes &score_end, uint64_t limit);
	virtual ZIterator* zrscan(const Bytes &name, const Bytes &key,
			const Bytes &score_start, const Bytes &score_end, uint64_t limit);
	virtual int zlist(const Bytes &name_s, const Bytes &name_e, uint64_t limit,
			std::vector<std::string> *list);
	
	virtual int64_t qsize(const Bytes &name);
	// @return 0: empty queue, 1: item peeked, -1: error
	virtual int qfront(const Bytes &name, std::string *item);
	// @return 0: empty queue, 1: item peeked, -1: error
	virtual int qback(const Bytes &name, std::string *item);
	// @return -1: error, 1: item added
	virtual int qpush_front(const Bytes &name, const Bytes &item);
	virtual int qpush_back(const Bytes &name, const Bytes &item);
	// @return 0: empty queue, 1: item popped, -1: error
	virtual int qpop_front(const Bytes &name, std::string *item);
	virtual int qpop_back(const Bytes &name, std::string *item);
	virtual int qfix(const Bytes &name);
	virtual int qlist(const Bytes &name_s, const Bytes &name_e, uint64_t limit,
			std::vector<std::string> *list);

private:
	int _qpush(const Bytes &name, const Bytes &item, uint64_t front_or_back_seq);
	int _qpop(const Bytes &name, std::string *item, uint64_t front_or_back_seq);
};


}; // end namespace ssdb

#endif
