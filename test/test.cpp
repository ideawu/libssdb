#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/time.h>
#include <vector>
#include <string>
#include "ssdb/ssdb.h"

double millitime();

int main(int argc, char **argv){
	srand(time(NULL));

	ssdb::Options options;
	ssdb::Db *db;
	
	options.path = "./tmp";
	options.cache_size = 8;
	options.compression = true;
	
	db = ssdb::Db::open(options);
	if(!db){
		fprintf(stderr, "Open database failed!\n");
		exit(0);
	}
	
	int MAX_NUM = 10000;
	int VALUE_LEN = 100;
	
	{
		// prepare data
		std::vector<std::string> keys;
		for(int i=0; i<MAX_NUM; i++){
			char buf[32];
			snprintf(buf, sizeof(buf), "%d", rand()%100000);
			keys.push_back(buf);
		}
		std::string val(VALUE_LEN, 'a');
		double stime = millitime();
		for(int i=0; i<MAX_NUM; i++){
			std::string &key = keys[i];
			db->set(key, val);
		}
		double etime = millitime();
		double ts = etime - stime;
		if(ts == 0){
			ts = 1;
		}
		int num = MAX_NUM;
		double us_per_op = ts/num * 1000 * 1000;
		double mbs = (double)(VALUE_LEN * num)/ts/1024/1024;
		int iops = (int)((double)num/ts);
		printf("write: %10d op/s, %.3f us/op, %.2f MB/s, keys: %d\n", iops, us_per_op, mbs, num);
	}
	
	{
		double stime = millitime();
		ssdb::KIterator *it;
		it = db->scan("", "", -1);
		int num = 0;
		while(it->next()){
			num ++;
		}
		delete it;
		double etime = millitime();
		double ts = etime - stime;
		if(ts == 0){
			ts = 1;
		}
		double us_per_op = ts/num * 1000 * 1000;
		double mbs = (double)(VALUE_LEN * num)/ts/1024/1024;
		int iops = (int)((double)num/ts);
		printf("scan : %10d op/s, %.3f us/op, %.2f MB/s, keys: %d\n", iops, us_per_op, mbs, num);
	}
	
	delete db;
	return 0;
}

double millitime(){
	struct timeval now;
	gettimeofday(&now, NULL);
	double ret = now.tv_sec + now.tv_usec/1000.0/1000.0;
	return ret;
}
