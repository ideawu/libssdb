#ifndef SSDB_ITERATOR_H_
#define SSDB_ITERATOR_H_

#include <inttypes.h>
#include <string>
#include "bytes.h"

namespace ssdb{

class Iterator{
public:
	enum Direction{
		FORWARD, BACKWARD
	};

	Iterator(){}
	virtual ~Iterator(){}
	virtual bool skip(uint64_t offset) = 0;
	virtual bool next() = 0;
	virtual Bytes key() = 0;
	virtual Bytes val() = 0;
};


class KIterator{
public:
	std::string key;
	std::string val;

	KIterator(Iterator *it);
	~KIterator();
	void return_val(bool onoff);
	bool next();
private:
	Iterator *it;
	bool return_val_;
};


class HIterator{
public:
	std::string name;
	std::string key;
	std::string val;

	HIterator(Iterator *it, const Bytes &name);
	~HIterator();
	void return_val(bool onoff);
	bool next();
private:
	Iterator *it;
	bool return_val_;
};


class ZIterator{
public:
	std::string name;
	std::string key;
	std::string score;

	ZIterator(Iterator *it, const Bytes &name);
	~ZIterator();
	bool skip(uint64_t offset);
	bool next();
private:
	Iterator *it;
};

}; // end namespace ssdb

#endif
