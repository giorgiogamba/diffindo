#include "chunker.hpp"

#include <pqxx/pqxx>
#include <stdexcept>

namespace diffindo
{

void Chunker::split(std::vector<Chunk>& out) const 
{
    pqxx::work txn(m_connection);

    auto row = txn.exec1("SELECT MIN("+ txn.quote_name(m_idCol)+"), MAX("+
                txn.quote_name(m_idCol)+") FROM " + txn.quote_name(m_table));

    if (row[0].is_null())
        return;

    const int64_t minId = row[0].as<int64_t>();
    const int64_t maxId = row[1].as<int64_t>();

    out.clear();
    int64_t chunkId = 0;
    int64_t low = minId;

    while (low <= maxId)
    {
        const int64_t high = std::min(low + m_chunkSize - 1, maxId);
        out.push_back(Chunk{ chunkId ++, low, high });
        low = high + 1;
    }

    txn.commit();
}

}