#ifndef SSDB_BYTES_H_
#define SSDB_BYTES_H_

#include <inttypes.h>
#include <string>

namespace ssdb{

// readonly
// to replace std::string
class Bytes{
public:
	Bytes(){
		data_ = "";
		size_ = 0;
	}

	Bytes(void *data, int size){
		data_ = (char *)data;
		size_ = size;
	}

	Bytes(const char *data, int size){
		data_ = data;
		size_ = size;
	}

	Bytes(const std::string &str){
		data_ = str.data();
		size_ = str.size();
	}

	Bytes(const char *str){
		data_ = str;
		size_ = strlen(str);
	}

	const char* data() const{
		return data_;
	}

	bool empty() const{
		return size_ == 0;
	}

	int size() const{
		return size_;
	}

	int compare(const Bytes &b) const{
		const int min_len = (size_ < b.size_) ? size_ : b.size_;
		int r = memcmp(data_, b.data_, min_len);
		if(r == 0){
			if (size_ < b.size_) r = -1;
			else if (size_ > b.size_) r = +1;
		}
		return r;
	}

	std::string String() const{
		return std::string(data_, size_);
	}

	int Int() const;
	int64_t Int64() const;
	uint64_t Uint64() const;
	double Double() const;

private:
	const char *data_;
	int size_;
};

inline
bool operator==(const Bytes &x, const Bytes &y){
	return ((x.size() == y.size()) &&
			(memcmp(x.data(), y.data(), x.size()) == 0));
}

inline
bool operator!=(const Bytes &x, const Bytes &y){
	return !(x == y);
}

inline
bool operator>(const Bytes &x, const Bytes &y){
	return x.compare(y) > 0;
}

inline
bool operator>=(const Bytes &x, const Bytes &y){
	return x.compare(y) >= 0;
}

inline
bool operator<(const Bytes &x, const Bytes &y){
	return x.compare(y) < 0;
}

inline
bool operator<=(const Bytes &x, const Bytes &y){
	return x.compare(y) <= 0;
}

}; // end namespace ssdb

#endif

