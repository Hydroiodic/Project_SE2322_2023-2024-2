
LINK.o = $(LINK.cpp)
CXXFLAGS = -std=c++17 -Wall

ALLSRC := $(wildcard ./*.cpp ./**/*.cpp)
LIBSRC := $(filter-out %correctness.cpp %persistence.cpp, $(ALLSRC))
TARGET := $(patsubst %.cpp, %.o, $(ALLSRC))
LIBTARGET := $(patsubst %.cpp, %.o, $(LIBSRC))

all: $(TARGET) correctness persistence

correctness: correctness.o $(LIBTARGET)

persistence: persistence.o $(LIBTARGET)

clean: clear
	rm -f correctness persistence $(TARGET)

clear:
	rm -rf $(filter-out .gitkeep, ./data/*)
