#include ../build_config.mk

all: test.o
	g++ -O2 -o test \
		test.o \
		../output/lib/libleveldb.a ../output/lib/libsnappy.a ../output/lib/libssdb.a

test.o: test.cpp
	g++ -c -O2 -I ../output/include test.cpp

clean:
	rm -f *.o test
