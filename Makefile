CC = g++
CCFLAGS = -O3 -Wall -g -std=c++11
LEVELDB_DIR = ./leveldb/build
LEVELDB_OPTS = -I $(LEVELDB_DIR)/include -pthread $(LEVELDB_DIR)/libleveldb.a -lsnappy
JSONCPP_OPTS = -I .

all: library.o jsoncpp.o msort bsort

library.o: library.cc library.h
	$(CC) -o $@ -c $< $(CCFLAGS)

jsoncpp.o: jsoncpp.cpp json/json.h
	$(CC) -o $@ -c $< $(CCFLAGS) $(JSONCPP_OPTS)

msort: msort.cc library.o jsoncpp.o
	$(CC) -o $@ $^ $(CCFLAGS)

bsort: bsort.cc library.o jsoncpp.o
	$(CC) -o $@ $^ $(CCFLAGS) $(LEVELDB_OPTS)
	
clean:
	rm -rf *.o msort bsort msort.dSYM bsort.dSYM
