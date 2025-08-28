# 定义变量
CXX = g++
CXXFLAGS = -Wno-unused-result -Wno-unused-variable -std=c++20 -O3 -g -pthread

# 获取当前文件夹下的所有 .c 文件
SOURCES := $(wildcard *.cpp)
# SOURCES := $(filter-out test_search_engine.cpp, $(SOURCES))

# 将 .c 文件名替换为 .o 文件名
OBJECTS := $(SOURCES:.cpp=.o)

# 目标：默认目标是编译生成的可执行文件
all: $(OBJECTS)
	$(CXX) $(CXXFLAGS) -o lsm_program $(OBJECTS)

# 生成目标文件
%.o: %.c
	$(CXX) $(CXXFLAGS) -c $< -o $@

# 清除目标文件和可执行文件
clean:
	rm -f $(OBJECTS) lsm_program
