#include "ssdb/bytes.h"
#include "strings.h"

namespace ssdb{

int Bytes::Int() const{
	return str_to_int(data_, size_);
}

int64_t Bytes::Int64() const{
	return str_to_int64(data_, size_);
}

uint64_t Bytes::Uint64() const{
	return str_to_uint64(data_, size_);
}

double Bytes::Double() const{
	return str_to_double(data_, size_);
}

}; // end namespace ssdb
