#include "chunker/chunker.hpp"
#include <iostream>

int main() {
    std::string conn = "host=localhost port=5432 dbname=source user=drift password=drift";

    diffindo::Chunker chunker(conn, "accounts", "id", 500);

    std::vector<diffindo::Chunk> chunks;
    chunker.split(chunks);

    std::cout << "Total chunks: " << chunks.size() << "\n";
    for (const auto& c : chunks)
    {
        std::cout << "  chunk " << c.id << " ids [" << c.minId << ", " << c.maxId << "]\n";
    }
    return 0;
}