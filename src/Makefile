include ../build_config.mk

OBJS = db_impl.o t_kv.o t_hash.o t_zset.o t_queue.o \
	iterator_impl.o writer.o
UTIL_OBJS = util/bytes.o util/log.o
LIB = libssdb.a
EXES =

all: ${OBJS}
	cd util; ${MAKE}
	ar -cru $(LIB) $(OBJS) $(UTIL_OBJS)

db_impl.o: db_impl.h db_impl.cpp
	g++ ${CFLAGS} -c db_impl.cpp

iterator_impl.o: iterator_impl.h iterator_impl.cpp
	g++ ${CFLAGS} -c iterator_impl.cpp

t_kv.o: t_kv.h t_kv.cpp
	g++ ${CFLAGS} -c t_kv.cpp

t_hash.o: t_hash.h t_hash.cpp
	g++ ${CFLAGS} -c t_hash.cpp

t_zset.o: t_zset.h t_zset.cpp
	g++ ${CFLAGS} -c t_zset.cpp

t_queue.o: t_queue.h t_queue.cpp
	g++ ${CFLAGS} -c t_queue.cpp

writer.o: writer.h writer.cpp
	g++ ${CFLAGS} -c writer.cpp

clean:
	cd util; ${MAKE} clean
	rm -f build_config.mk
	rm -f ${EXES} *.o *.exe $(LIB)

