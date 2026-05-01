#include "hasher.hpp"

#include <pqxx/pqxx>
#include <xxhash.h>
#include <semaphore>
#include <thread>
#include <stdexcept>
#include <iostream>

namespace diffindo {

void ResultQueue::push(ChunkResult result)
{
    {
        std::lock_guard<std::mutex> lock(m_queueMtx);
        m_queue.push(std::move(result));
    }
    m_queueReady.notify_one();
}

std::optional<ChunkResult> ResultQueue::pop(int timeout_ms)
{
    std::unique_lock<std::mutex> lock(m_queueMtx);
    bool signalled = m_queueReady.wait_for(lock, std::chrono::milliseconds(timeout_ms), [this]
    {
        return !m_queue.empty() || m_closed;
    });

    if (!signalled || m_queue.empty())
        return std::nullopt;

    ChunkResult r = std::move(m_queue.front());
    m_queue.pop();
    return r;
}

void ResultQueue::close()
{
    {
        std::lock_guard<std::mutex> lock(m_queueMtx);
        m_closed = true;
    }
    m_queueReady.notify_all();
}


Hasher::Hasher(std::string_view conn_a,
               std::string_view conn_b,
               std::string_view table,
               std::string_view id_col,
               int              max_workers)
    : m_connectionA(conn_a)
    , m_connectionB(conn_b)
    , m_table(table)
    , m_colId(id_col)
    , m_maxWorkers(max_workers)
{}

std::pair<uint64_t, int64_t> Hasher::hashChunk( const std::string& conn_str, const Chunk& chunk) const
{
    pqxx::connection conn(conn_str);
    pqxx::work       txn(conn);

    std::string query =
        "SELECT * FROM " + txn.quote_name(m_table) +
        " WHERE "        + txn.quote_name(m_colId) +
        " BETWEEN "      + std::to_string(chunk.minId) +
        " AND "          + std::to_string(chunk.maxId) +
        " ORDER BY "     + txn.quote_name(m_colId);

    pqxx::result rows = txn.exec(query);

    XXH3_state_t* state = XXH3_createState();
    XXH3_64bits_reset(state);

    for (const auto& row : rows)
    {
        for (const auto& field : row)
        {
            std::string val = field.is_null() ? "\x00NULL\x00" : field.c_str();
            XXH3_64bits_update(state, val.data(), val.size());
            XXH3_64bits_update(state, "|", 1);
        }

        XXH3_64bits_update(state, "\n", 1);
    }

    uint64_t hash = XXH3_64bits_digest(state);
    XXH3_freeState(state);
    txn.commit();

    return { hash, static_cast<int64_t>(rows.size()) };
}

void Hasher::hashChunks(const std::vector<Chunk>& chunks, ResultQueue& outQueue)
{
    if (chunks.empty())
        return;

    // Cap concurrency at m_maxWorkers
    std::counting_semaphore<256> sem(m_maxWorkers);

    std::vector<std::jthread> workers;
    workers.reserve(chunks.size());

    for (const auto& chunk : chunks)
    {
        sem.acquire();

        workers.emplace_back([this, &chunk, &outQueue, &sem]()
        {
            try
            {
                auto [hashA, countA] = hashChunk(m_connectionA, chunk);
                auto [hashB, countB] = hashChunk(m_connectionB, chunk);

                ChunkResult result {
                    .chunkId    = chunk.id,
                    .minId      = chunk.minId,
                    .maxId      = chunk.maxId,
                    .hashA      = hashA,
                    .hashB      = hashB,
                    .countA     = countA,
                    .countB     = countB,
                    .diverged   = (hashA != hashB || countA != countB)
                };

                outQueue.push(std::move(result));
            }
            catch (const std::exception& e)
            {
                std::cerr << "[worker] chunk " << chunk.id << " error: " << e.what() << "\n";
            }

            sem.release();
        });
    }

    // jthread joins automatically on destruction — this is the barrier
    workers.clear();
}

} // namespace diffindo