CXX := g++
CXXFLAGS := -std=c++17 -Wall -Wextra -O2 -Iinclude -pthread

SRC := src/wal.cpp src/store.cpp src/replicator.cpp src/raft.cpp src/server.cpp
OBJ := $(SRC:.cpp=.o)
BIN := kvserver

all: $(BIN)

$(BIN): $(OBJ)
	$(CXX) $(CXXFLAGS) -o $@ $(OBJ)

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm -f $(OBJ) $(BIN)
	rm -f data/wal.log

.PHONY: all clean
