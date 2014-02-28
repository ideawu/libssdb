#ifndef SSDB_H_
#define SSDB_H_

#include <string>
#include <vector>
#include <inttypes.h>

#include "bytes.h"
#include "options.h"
#include "iterator.h"

namespace ssdb{

class Db
{
public:
	static Db* open(const Options &options);
	Db(){};
	virtual ~Db(){};

	/* key value */

	virtual int set(const Bytes &key, const Bytes &val) = 0;
	virtual int del(const Bytes &key) = 0;
	virtual int incr(const Bytes &key, int64_t by, std::string *new_val) = 0;
	virtual int multi_set(const std::vector<Bytes> &kvs, int offset=0) = 0;
	virtual int multi_del(const std::vector<Bytes> &keys, int offset=0) = 0;
	
	virtual int get(const Bytes &key, std::string *val) = 0;
	// return (start, end]
	virtual KIterator* scan(const Bytes &start, const Bytes &end, uint64_t limit) = 0;
	virtual KIterator* rscan(const Bytes &start, const Bytes &end, uint64_t limit) = 0;

	/* hash */

	virtual int hset(const Bytes &name, const Bytes &key, const Bytes &val) = 0;
	virtual int hdel(const Bytes &name, const Bytes &key) = 0;
	virtual int hincr(const Bytes &name, const Bytes &key, int64_t by, std::string *new_val) = 0;
	//int multi_hset(const Bytes &name, const std::vector<Bytes> &kvs, int offset=0) = 0;
	//int multi_hdel(const Bytes &name, const std::vector<Bytes> &keys, int offset=0) = 0;

	virtual int64_t hsize(const Bytes &name) = 0;
	virtual int hget(const Bytes &name, const Bytes &key, std::string *val) = 0;
	virtual int hlist(const Bytes &name_s, const Bytes &name_e, uint64_t limit,
			std::vector<std::string> *list) = 0;
	virtual HIterator* hscan(const Bytes &name, const Bytes &start, const Bytes &end, uint64_t limit) = 0;
	virtual HIterator* hrscan(const Bytes &name, const Bytes &start, const Bytes &end, uint64_t limit) = 0;

	/* zset */

	virtual int zset(const Bytes &name, const Bytes &key, const Bytes &score) = 0;
	virtual int zdel(const Bytes &name, const Bytes &key) = 0;
	virtual int zincr(const Bytes &name, const Bytes &key, int64_t by, std::string *new_val) = 0;
	//int multi_zset(const Bytes &name, const std::vector<Bytes> &kvs, int offset=0) = 0;
	//int multi_zdel(const Bytes &name, const std::vector<Bytes> &keys, int offset=0) = 0;
	
	virtual int64_t zsize(const Bytes &name) = 0;
	/**
	 * @return -1: error; 0: not found; 1: found
	 */
	virtual int zget(const Bytes &name, const Bytes &key, std::string *score) = 0;
	virtual int64_t zrank(const Bytes &name, const Bytes &key) = 0;
	virtual int64_t zrrank(const Bytes &name, const Bytes &key) = 0;
	virtual ZIterator* zrange(const Bytes &name, uint64_t offset, uint64_t limit) = 0;
	virtual ZIterator* zrrange(const Bytes &name, uint64_t offset, uint64_t limit) = 0;
	/**
	 * scan by score, but won't return @key if key.score=score_start.
	 * return (score_start, score_end]
	 */
	virtual ZIterator* zscan(const Bytes &name, const Bytes &key,
			const Bytes &score_start, const Bytes &score_end, uint64_t limit) = 0;
	virtual ZIterator* zrscan(const Bytes &name, const Bytes &key,
			const Bytes &score_start, const Bytes &score_end, uint64_t limit) = 0;
	virtual int zlist(const Bytes &name_s, const Bytes &name_e, uint64_t limit,
			std::vector<std::string> *list) = 0;
	
	virtual int64_t qsize(const Bytes &name) = 0;
	// @return 0: empty queue, 1: item peeked, -1: error
	virtual int qfront(const Bytes &name, std::string *item) = 0;
	// @return 0: empty queue, 1: item peeked, -1: error
	virtual int qback(const Bytes &name, std::string *item) = 0;
	// @return -1: error, 1: item added
	virtual int qpush_front(const Bytes &name, const Bytes &item) = 0;
	virtual int qpush_back(const Bytes &name, const Bytes &item) = 0;
	// @return 0: empty queue, 1: item popped, -1: error
	virtual int qpop_front(const Bytes &name, std::string *item) = 0;
	virtual int qpop_back(const Bytes &name, std::string *item) = 0;
	virtual int qfix(const Bytes &name) = 0;
	virtual int qlist(const Bytes &name_s, const Bytes &name_e, uint64_t limit,
			std::vector<std::string> *list) = 0;


	// return (start, end], not include start
	virtual Iterator* iterator(const std::string &start, const std::string &end, uint64_t limit) = 0;
	virtual Iterator* rev_iterator(const std::string &start, const std::string &end, uint64_t limit) = 0;

	//void flushdb() = 0;
	virtual std::vector<std::string> info() = 0;
	virtual void compact() = 0;
	virtual int key_range(std::vector<std::string> *keys) = 0;

	/* raw operates */

	virtual int raw_set(const Bytes &key, const Bytes &val) = 0;
	virtual int raw_del(const Bytes &key) = 0;
	virtual int raw_get(const Bytes &key, std::string *val) = 0;
};

}; // end namespace ssdb

#endif
