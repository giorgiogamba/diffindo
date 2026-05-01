#include "chunker/chunker.hpp"
#include "hasher/hasher.hpp"
#include <iostream>

int main()
{
    std::string connectionA = "host=localhost port=5432 dbname=source user=drift password=drift";
    std::string connectionB = "host=localhost port=5433 dbname=replica user=drift password=drift";

    diffindo::Chunker chunker(connectionA, "accounts", "id", 500);
    std::vector<diffindo::Chunk> chunks;
    chunker.split(chunks);
    std::cout << "Total chunks: " << chunks.size() << "\n";

    // Step 2 — hash all chunks in parallel
    diffindo::Hasher hasher(connectionA, connectionB, "accounts", "id", 4);
    diffindo::ResultQueue queue;
    hasher.hashChunks(chunks, queue);
    queue.close();

    // Step 3 — drain the queue and print results
    int diverged = 0;
    while (true) {
        auto result = queue.pop(500);
        if (!result) break;

        std::cout << "  chunk " << result->chunkId
                  << " ids [" << result->minId << ", " << result->maxId << "]"
                  << " diverged=" << (result->diverged ? "yes" : "no")
                  << " rows_a=" << result->countA
                  << " rows_b=" << result->countB
                  << "\n";

        if (result->diverged) ++diverged;
    }

    std::cout << "\nTotal diverged chunks: " << diverged
              << " / " << chunks.size() << "\n";

    return 0;
}