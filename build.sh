#!/bin/sh
BASE_DIR=`pwd`
TARGET_OS=`uname -s`
LEVELDB_PATH="$BASE_DIR/deps/leveldb-1.14.0"
SNAPPY_PATH="$BASE_DIR/deps/snappy-1.1.0"
MAKE=make

case "$TARGET_OS" in
    Darwin)
        #PLATFORM_CLIBS="-pthread"
		#PLATFORM_CFLAGS=""
        ;;
    Linux)
        PLATFORM_CLIBS="-pthread"
        ;;
    CYGWIN_*)
        PLATFORM_CLIBS="-lpthread"
        ;;
    SunOS)
        PLATFORM_CLIBS="-lpthread -lrt"
        ;;
    FreeBSD)
        PLATFORM_CLIBS="-lpthread"
		MAKE=gmake
        ;;
    NetBSD)
        PLATFORM_CLIBS="-lpthread -lgcc_s"
        ;;
    OpenBSD)
        PLATFORM_CLIBS="-pthread"
        ;;
    DragonFly)
        PLATFORM_CLIBS="-lpthread"
        ;;
    HP-UX)
        PLATFORM_CLIBS="-pthread"
        ;;
    *)
        echo "Unknown platform!" >&2
        exit 1
esac


DIR=`pwd`
cd $SNAPPY_PATH
if [ ! -f Makefile ]; then
	echo ""
	echo "##### building snappy... #####"
	./configure
	# FUCK! snappy compilation doesn't work on some linux!
	find . | xargs touch
	make
	echo "##### building snappy finished #####"
	echo ""
fi
cd "$DIR"


rm -f build_config.mk
echo "MAKE=$MAKE" >> build_config.mk
echo "LEVELDB_PATH=$LEVELDB_PATH" >> build_config.mk
echo "SNAPPY_PATH=$SNAPPY_PATH" >> build_config.mk

echo "CFLAGS=" >> build_config.mk
echo "CFLAGS = -DNDEBUG -D__STDC_FORMAT_MACROS -Wall -O2 -Wno-sign-compare" >> build_config.mk
echo "CFLAGS += ${PLATFORM_CFLAGS}" >> build_config.mk
echo "CFLAGS += -I \"$LEVELDB_PATH/include\"" >> build_config.mk
echo "CFLAGS += -I \"$BASE_DIR/src/include\"" >> build_config.mk

echo "CLIBS=" >> build_config.mk
echo "CLIBS += ${PLATFORM_CLIBS}" >> build_config.mk
echo "CLIBS += \"$LEVELDB_PATH/libleveldb.a\"" >> build_config.mk
echo "CLIBS += \"$SNAPPY_PATH/.libs/libsnappy.a\"" >> build_config.mk

