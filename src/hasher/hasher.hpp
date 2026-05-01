#pragma once

#include "chunker/chunker.hpp"

#include <cstdint>
#include <string>
#include <vector>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <optional>
#include <semaphore>
#include <thread>

namespace diffindo {

struct ChunkResult
{
    int64_t  chunkId;
    int64_t  minId;
    int64_t  maxId;
    uint64_t hashA;
    uint64_t hashB;
    int64_t  countA;
    int64_t  countB;
    bool     diverged;
};

class ResultQueue
{
public:
    void push(ChunkResult result);
    std::optional<ChunkResult> pop(int timeoutMS = 1000);
    void close();

private:
    std::queue<ChunkResult>  m_queue;
    std::mutex               m_queueMtx;
    std::condition_variable  m_queueReady;
    bool                     m_closed = false;
};

class Hasher
{
public:
    explicit Hasher(std::string_view connectionA,
                    std::string_view connectionB,
                    std::string_view table,
                    std::string_view colId,
                    int              maxWorkers);

    Hasher(const Hasher&)            = delete;
    Hasher& operator=(const Hasher&) = delete;

    void hashChunks(const std::vector<Chunk>& chunks, ResultQueue& outQueue);

private:
    std::string m_connectionA;
    std::string m_connectionB;
    std::string m_table;
    std::string m_colId;
    int         m_maxWorkers;

    std::pair<uint64_t, int64_t> hashChunk(const std::string& conn_str, const Chunk& chunk) const;
};

} // namespace diffindo