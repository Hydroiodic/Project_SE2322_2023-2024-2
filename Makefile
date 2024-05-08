
CXX = clang++
LINK.o = $(LINK.cpp)
CXXFLAGS = -std=c++17 -Wall -Ofast

ALLSRC := $(wildcard ./*.cpp ./**/*.cpp)
LIBSRC := $(filter-out ./correctness.cpp ./persistence.cpp ./test/%.cpp, $(ALLSRC))
TARGET := $(patsubst %.cpp, %.o, $(ALLSRC))
LIBTARGET := $(patsubst %.cpp, %.o, $(LIBSRC))

all: $(TARGET) correctness persistence

correctness: correctness.o $(LIBTARGET)

persistence: persistence.o $(LIBTARGET)

basic: test/basic.o $(LIBTARGET)
	$(LINK.o) $^ $(LOADLIBES) $(LDLIBS) -o $@

cache: test/cache.o $(LIBTARGET)
	$(LINK.o) $^ $(LOADLIBES) $(LDLIBS) -o $@

compaction: test/compaction.o $(LIBTARGET)
	$(LINK.o) $^ $(LOADLIBES) $(LDLIBS) -o $@

filter: test/filter.o $(LIBTARGET)
	$(LINK.o) $^ $(LOADLIBES) $(LDLIBS) -o $@

clean: clear
	rm -f correctness persistence basic cache compaction filter $(TARGET)

clear:
	rm -rf $(filter-out .gitkeep, ./data/*)
