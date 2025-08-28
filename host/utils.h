#ifndef UTILS_H
#define UTILS_H
#include <cstdint>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <fcntl.h>
#include <unistd.h>
#include <assert.h>
#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <functional>
#include <sys/time.h>
enum DataType { UINT32 = 0, UINT64 = 1 };
uint64_t readStorage(int fd, void* buffer, uint64_t size);
uint64_t writeStorage(int fd, void* buffer, size_t size);
DataType resolve_type(const std::string& filename);

template <class KeyType = uint64_t>
struct EqualityLookup {
  KeyType key;
  uint64_t result;
};

template <typename T>
static std::vector<T> load_data(const std::string& filename,
                                bool print = true) {
    std::vector<T> data;
    std::ifstream in(filename, std::ios::binary);
    if (!in.is_open()) {
        std::cerr << "unable to open " << filename << std::endl;
         exit(EXIT_FAILURE);
    }
    // Read size.
    uint64_t size;
    in.read(reinterpret_cast<char*>(&size), sizeof(uint64_t));
    data.resize(size);
    // Read values.
    in.read(reinterpret_cast<char*>(data.data()), size * sizeof(T));
    in.close();

    return data;
}
uint64_t get_time_us();
extern char openssd_dir[];
extern char mnt_dir[];
#endif