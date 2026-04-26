#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include <pqxx/pqxx>

namespace diffindo
{

struct Chunk
{
    int64_t id;
    int64_t minId;
    int64_t maxId;
};

class Chunker
{
public:

    explicit Chunker ( const std::string_view& connection
            , const std::string_view& table
            , const std::string_view& idCol
            , const int64_t chunkSize )
            : m_connection { std::string{ connection } }
            , m_table { table }
            , m_idCol { idCol }
            , m_chunkSize { chunkSize }
        {}

    // A single Chunker owns a DB connection
    Chunker(const Chunker&) = delete;
    Chunker& operator=(const Chunker&) = delete;

    Chunker(Chunker&&) = default;
    Chunker& operator=(Chunker&&) = default;

    void split(std::vector<Chunk>& out) const;

private:

    mutable pqxx::connection m_connection;
    std::string m_table;
    std::string m_idCol;
    int64_t m_chunkSize;
};


}