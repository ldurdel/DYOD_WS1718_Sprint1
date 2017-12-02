#include <memory>
#include <random>
#include <string>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "operators/get_table.hpp"
#include "operators/table_scan/table_scan.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

class PerformanceTableScanTest : public BaseTest {
 protected:
  static void SetUpTestCase() {
    _random_table_small = std::make_shared<Table>(_entries_small / _number_of_chunks);
    _sorted_table_small = std::make_shared<Table>(_entries_small / _number_of_chunks);
    _random_table_medium = std::make_shared<Table>(_entries_medium / _number_of_chunks);
    _sorted_table_medium = std::make_shared<Table>(_entries_medium / _number_of_chunks);
    _random_table_large = std::make_shared<Table>(_entries_large / _number_of_chunks);
    _sorted_table_large = std::make_shared<Table>(_entries_large / _number_of_chunks);

    _fill_with_sorted_values(_sorted_table_small, _entries_small);
    _fill_with_sorted_values(_sorted_table_medium, _entries_medium);
    _fill_with_sorted_values(_sorted_table_large, _entries_large);

    _fill_with_random_values(_random_table_small, _entries_small);
    _fill_with_random_values(_random_table_medium, _entries_medium);
    _fill_with_random_values(_random_table_large, _entries_large);

    // Compress every second chunk to evaluate both Value- and DictionaryColumn performance
    for (ChunkID chunk_id{0}; chunk_id < _number_of_chunks; chunk_id += 2) {
      _sorted_table_small->compress_chunk(chunk_id);
      _sorted_table_medium->compress_chunk(chunk_id);
      _sorted_table_large->compress_chunk(chunk_id);

      _random_table_small->compress_chunk(chunk_id);
      _random_table_medium->compress_chunk(chunk_id);
      _random_table_large->compress_chunk(chunk_id);
    }
  }

  static void TearDownTestCase() {
    std::vector<std::string> table_names = {"randomSmall",  "sortedSmall", "randomMedium",
                                            "sortedMedium", "randomLarge", "sortedLarge"};

    for (auto table_name : table_names) {
      if (StorageManager::get().has_table(table_name)) {
        StorageManager::get().drop_table(table_name);
      }
    }

    _random_table_small.reset();
    _sorted_table_small.reset();
    _random_table_medium.reset();
    _sorted_table_medium.reset();
    _random_table_large.reset();
    _sorted_table_large.reset();
  }

  void SetUp() override {
    // BaseTest destructor automatically calls StorageManager::reset()
    // so we have to re-initialize the StorageManager for every test
    StorageManager::get().add_table("randomSmall", _random_table_small);
    StorageManager::get().add_table("sortedSmall", _sorted_table_small);
    StorageManager::get().add_table("randomMedium", _random_table_medium);
    StorageManager::get().add_table("sortedMedium", _sorted_table_medium);
    StorageManager::get().add_table("randomLarge", _random_table_large);
    StorageManager::get().add_table("sortedLarge", _sorted_table_large);
  }

  // Simulates duplicate and sorted values. Fills the table with one column of unsigned integers,
  // duplicating 10 values before proceeding to the next one.
  static void _fill_with_sorted_values(std::shared_ptr<Table> table, int numberOfValues) {
    table->add_column("testColumn", "int");

    int currentValue = 0;

    for (auto index = 0; index < numberOfValues; ++index) {
      if (index % 10 == 0) {
        ++currentValue;
      }
      table->append({currentValue});
    }
  }

  // Simulates unsorted and random values. Fills the table with one column of unsigned, random integer values
  static void _fill_with_random_values(std::shared_ptr<Table> table, int numberOfValues) {
    table->add_column("testColumn", "int");

    // https://stackoverflow.com/a/19728404
    std::random_device random_device;
    std::mt19937 random_number_generator(random_device());
    std::uniform_int_distribution<int> distribution(0, numberOfValues);

    for (auto index = 0; index < numberOfValues; ++index) {
      table->append({distribution(random_number_generator)});
    }
  }

  void _scan(std::string table_name, ScanType scan_type, int threshold) {
    auto get_table = std::make_shared<GetTable>(table_name);
    get_table->execute();
    auto table_scan = std::make_shared<TableScan>(get_table, ColumnID{0}, scan_type, threshold);
    table_scan->execute();
  }

  // Defines the number of entries for each type of table.
  static const int _entries_small = 10000;
  static const int _entries_medium = 1000000;
  static const int _entries_large = 100000000;

  static const int _number_of_chunks = 10;

  static std::shared_ptr<Table> _random_table_small;
  static std::shared_ptr<Table> _sorted_table_small;
  static std::shared_ptr<Table> _random_table_medium;
  static std::shared_ptr<Table> _sorted_table_medium;
  static std::shared_ptr<Table> _random_table_large;
  static std::shared_ptr<Table> _sorted_table_large;
};

std::shared_ptr<Table> PerformanceTableScanTest::_random_table_small;
std::shared_ptr<Table> PerformanceTableScanTest::_sorted_table_small;
std::shared_ptr<Table> PerformanceTableScanTest::_random_table_medium;
std::shared_ptr<Table> PerformanceTableScanTest::_sorted_table_medium;
std::shared_ptr<Table> PerformanceTableScanTest::_random_table_large;
std::shared_ptr<Table> PerformanceTableScanTest::_sorted_table_large;

// Test GreaterThanEquals with ~50% of values as expected return set

TEST_F(PerformanceTableScanTest, GreaterThanRandomSmall50Perc) {
  _scan("randomSmall", ScanType::OpGreaterThanEquals, _entries_small / 2);
}

TEST_F(PerformanceTableScanTest, GreaterThanSortedSmall50Perc) {
  _scan("sortedSmall", ScanType::OpGreaterThanEquals, _entries_small / 20);
}

TEST_F(PerformanceTableScanTest, GreaterThanRandomMedium50Perc) {
  _scan("randomMedium", ScanType::OpGreaterThanEquals, _entries_medium / 2);
}

TEST_F(PerformanceTableScanTest, GreaterThanSortedMedium50Perc) {
  _scan("sortedMedium", ScanType::OpGreaterThanEquals, _entries_medium / 20);
}

TEST_F(PerformanceTableScanTest, GreaterThanRandomLarge50Perc) {
  _scan("randomLarge", ScanType::OpGreaterThanEquals, _entries_large / 2);
}

TEST_F(PerformanceTableScanTest, GreaterThanSortedLarge50Perc) {
  _scan("sortedLarge", ScanType::OpGreaterThanEquals, _entries_large / 20);
}

// Test 95% on large table

TEST_F(PerformanceTableScanTest, GreaterThanRandomLarge95Perc) {
  _scan("randomLarge", ScanType::OpGreaterThanEquals, _entries_large / 20);
}

TEST_F(PerformanceTableScanTest, GreaterThanSortedLarge95Perc) {
  _scan("sortedLarge", ScanType::OpGreaterThanEquals, _entries_large / 200);
}

// Test 5% on large table

TEST_F(PerformanceTableScanTest, GreaterThanRandomLarge05Perc) {
  _scan("randomLarge", ScanType::OpGreaterThanEquals, _entries_large * 19 / 20);
}

TEST_F(PerformanceTableScanTest, GreaterThanSortedLarge05Perc) {
  _scan("sortedLarge", ScanType::OpGreaterThanEquals, _entries_large * 19 / 200);
}

}  // namespace opossum
