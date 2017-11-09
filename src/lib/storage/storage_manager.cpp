#include "storage_manager.hpp"

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "utils/assert.hpp"

namespace opossum {

StorageManager& StorageManager::get() {
  static StorageManager _instance;
  return _instance;
}

void StorageManager::add_table(const std::string& name, std::shared_ptr<Table> table) {
  Assert(!has_table(name), "Duplicate table name");
  _tables.emplace(name, table);
}

void StorageManager::drop_table(const std::string& name) {
  Assert(has_table(name), "table does not exist");
  _tables.erase(name);
}

std::shared_ptr<Table> StorageManager::get_table(const std::string& name) const { return _tables.at(name); }

bool StorageManager::has_table(const std::string& name) const { return _tables.count(name) > 0; }

std::vector<std::string> StorageManager::table_names() const {
  std::vector<std::string> names;
  std::transform(_tables.cbegin(), _tables.cend(), names.begin(),
                 [](const auto& key_value) { return key_value.first; });
  return names;
}

void StorageManager::print(std::ostream& out) const {
  for (const auto& key_value : _tables) {
    const std::string& name = key_value.first;
    const Table& table = *(key_value.second);
    out << name << "\t" << table.col_count() << "\t" << table.row_count() << "\t" << table.chunk_count() << "\n";
  }
}

void StorageManager::reset() { get() = StorageManager{}; }

}  // namespace opossum
