#ifndef SSDB_ITERATOR_IMPL_H_
#define SSDB_ITERATOR_IMPL_H_

#include "leveldb/iterator.h"
#include "ssdb/iterator.h"

namespace ssdb{

class IteratorImpl : public Iterator{
public:
	IteratorImpl(leveldb::Iterator *it,
			const std::string &end,
			uint64_t limit,
			Direction direction=Iterator::FORWARD);
	virtual ~IteratorImpl();
	virtual bool skip(uint64_t offset);
	virtual bool next();
	virtual Bytes key();
	virtual Bytes val();
private:
	leveldb::Iterator *it;
	std::string end;
	uint64_t limit;
	bool is_first;
	int direction;
};

}; // end namespace ssdb

#endif
