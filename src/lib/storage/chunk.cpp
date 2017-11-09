#include <iomanip>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "base_column.hpp"
#include "chunk.hpp"

#include "utils/assert.hpp"

namespace opossum {

void Chunk::add_column(std::shared_ptr<BaseColumn> column) { _columns.push_back(column); }

void Chunk::append(const std::vector<AllTypeVariant>& values) {
  DebugAssert(values.size() == _columns.size(), "Data row does not match column layout");
  for (size_t i_column = 0; i_column < _columns.size(); ++i_column) {
    _columns[i_column]->append(values[i_column]);
  }
}

std::shared_ptr<BaseColumn> Chunk::get_column(ColumnID column_id) const { return _columns.at(column_id); }

uint16_t Chunk::col_count() const { return _columns.size(); }

uint32_t Chunk::size() const {
  if (_columns.empty()) {
    return 0;
  }

  DebugAssert(_columns.front(), "There should be a column, but it is null");
  return _columns.front()->size();
}

}  // namespace opossum
