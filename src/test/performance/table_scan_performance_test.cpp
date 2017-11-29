#include <memory>
#include <random>
#include <string>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "operators/get_table.hpp"
#include "operators/table_scan.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

class PerformanceTableScanTest : public BaseTest {
 protected:
  static void SetUpTestCase() {
    _random_table_small = std::make_shared<Table>(_entries_small / 10);
    _sorted_table_small = std::make_shared<Table>(_entries_small / 10);
    _random_table_medium = std::make_shared<Table>(_entries_medium / 10);
    _sorted_table_medium = std::make_shared<Table>(_entries_medium / 10);
    _random_table_large = std::make_shared<Table>(_entries_large / 10);
    _sorted_table_large = std::make_shared<Table>(_entries_large / 10);

    _fill_with_sorted_values(_sorted_table_small, _entries_small);
    _fill_with_sorted_values(_sorted_table_medium, _entries_medium);
    _fill_with_sorted_values(_sorted_table_large, _entries_large);

    _fill_with_random_values(_random_table_small, _entries_small);
    _fill_with_random_values(_random_table_medium, _entries_medium);
    _fill_with_random_values(_random_table_large, _entries_large);
  }

  static void TearDownTestCase() {
    std::vector<std::string> table_names = {"randomSmall",  "sortedSmall", "randomMedium",
                                            "sortedMedium", "randomLarge", "sortedLarge"};

    for (auto table_name : table_names) {
      if (StorageManager::get().has_table(table_name)) {
        StorageManager::get().drop_table(table_name);
      }
    }

    // ToDo arthur: destroy tables held by static pointers
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
  // From the range of 0 to 1000000.
  static void _fill_with_random_values(std::shared_ptr<Table> table, int numberOfValues) {
    table->add_column("testColumn", "int");

    // https://stackoverflow.com/a/19728404
    std::random_device random_device;
    std::mt19937 random_number_generator(random_device());
    std::uniform_int_distribution<int> distribution(0, 1000000);

    for (auto index = 0; index < numberOfValues; ++index) {
      table->append({distribution(random_number_generator)});
    }
  }

  void _scan_greater_than(std::string table_name, int threshold) {
    auto get_table = std::make_shared<GetTable>(table_name);
    get_table->execute();
    auto table_scan = std::make_shared<TableScan>(get_table, ColumnID{0}, ScanType::OpGreaterThanEquals, threshold);
    table_scan->execute();
  }

  // Defines the number of entries for each type of table.
  // There will be number/10 entries per chunk
  const static int _entries_small = 100;
  const static int _entries_medium = 10000;
  const static int _entries_large = 1000000;

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

TEST_F(PerformanceTableScanTest, GreaterThanRandomSmall) { _scan_greater_than("randomSmall", 500000); }

TEST_F(PerformanceTableScanTest, GreaterThanSortedSmall) { _scan_greater_than("sortedSmall", 5); }

TEST_F(PerformanceTableScanTest, GreaterThanRandomMedium) { _scan_greater_than("randomMedium", 500000); }

TEST_F(PerformanceTableScanTest, GreaterThanSortedMedium) { _scan_greater_than("sortedMedium", 50); }

TEST_F(PerformanceTableScanTest, GreaterThanRandomLarge) { _scan_greater_than("randomLarge", 500000); }

TEST_F(PerformanceTableScanTest, GreaterThanSortedLarge) { _scan_greater_than("sortedLarge", 50); }

}  // namespace opossum
