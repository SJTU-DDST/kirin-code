#include "utils.h"
#include <cstdint>
#include <string.h>
char openssd_dir[] = "/dev/nvme0n1";
char mnt_dir[] = "/mnt/openssd";
uint64_t readStorage(int fd, void* buffer, uint64_t size)
{
    uint64_t hasRead;
    if(size % 4096 != 0)
    {
        size = (size / 4096 + 1) * 4096;
    }
    hasRead = read(fd, buffer, size);
    // assert(hasRead > 0);
    // printf("%d, %ld, %ld\n",fd, size, hasRead);
    // assert(size == hasRead);
    return hasRead;
}

uint64_t writeStorage(int fd, void* buffer, size_t size)
{
    size_t written;
    size_t size_align64 = size;
    if(size % 4096 != 0)
    {
        size_align64 = (size / 4096 + 1) * 4096;
    }
    written = write(fd, buffer, size_align64);
    // printf("%d, %ld, %ld\n",fd, size, written);
    if(size_align64 != written)
    {
        printf("fd = %d\n", fd);
        perror("Write Error");
    }
    assert(size_align64 == written);
    return written;
}

static std::string get_suffix(const std::string& filename) {
    const std::size_t pos = filename.find_last_of("_");
    if (pos == filename.size() - 1 || pos == std::string::npos) return "";
    return filename.substr(pos + 1);
}

DataType resolve_type(const std::string& filename) {
    const std::string suffix = get_suffix(filename);
    if (suffix == "uint32.txt") {
        return DataType::UINT32;
    } else if (suffix == "uint64.txt") {
        return DataType::UINT64;
    } else {
        std::cerr << "type " << suffix << " not supported" << std::endl;
        exit(EXIT_FAILURE);
    }
}

uint64_t get_time_us() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (uint64_t)tv.tv_sec * 1000000 + tv.tv_usec;
}

// template <class T>
// static std::vector<T> load_data(const std::string& filename) {
//     std::vector<T> data;
//     std::ifstream in(filename, std::ios::binary);
//     if (!in.is_open()) {
//         std::cerr << "unable to open " << filename << std::endl;
//         exit(EXIT_FAILURE);
//     }
//     // Read size.
//     uint64_t size;
//     in.read(reinterpret_cast<char*>(&size), sizeof(uint64_t));
//     std::cout << "size = " << size << "\n";
//     data.resize(size);
//     uint64_t result;
//     // Read values.
//     for(int i = 0; i < size; i++)
//     {
//         in.read(reinterpret_cast<char*>(&data[i]), sizeof(T));
//         in.read(reinterpret_cast<char*>(&result), sizeof(uint64_t));
//     }
//     in.close();
//     return data;
// }