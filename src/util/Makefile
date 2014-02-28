include ../../build_config.mk

OBJS = bytes.o log.o
EXES = 

all: ${OBJS}

bytes.o: bytes.cpp
	g++ ${CFLAGS} -c bytes.cpp

log.o: log.h log.cpp
	g++ ${CFLAGS} -c log.cpp


clean:
	rm -f ${EXES} ${OBJS}

