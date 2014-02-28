#ifndef SSDB_OPTIONS_H_
#define SSDB_OPTIONS_H_

namespace ssdb{

struct Options{
	std::string path;

	// In MBs.
	int cache_size;
	// Default: false
	bool compression;
	
	Options(){
		cache_size = 8;
		compression = false;
	}
};

}; // end namespace ssdb

#endif