PREFIX=/usr/local/ssdb
OUTPUT=./output

$(shell sh build.sh 1>&2)
include build_config.mk

all:
	chmod u+x "$(LEVELDB_PATH)/build_detect_platform"
	cd "$(LEVELDB_PATH)"; $(MAKE)
	cd src; $(MAKE)
	#
	mkdir -p $(OUTPUT)/lib
	cp -r src/include $(OUTPUT)
	cp src/libssdb.a $(OUTPUT)/lib
	cp $(LEVELDB_PATH)/libleveldb.a $(OUTPUT)/lib
	cp $(SNAPPY_PATH)/.libs/libsnappy.a $(OUTPUT)/lib

clean:
	rm -f *.exe.stackdump
	cd src; $(MAKE) clean

