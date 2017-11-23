#include "value_column.hpp"

#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "type_cast.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

namespace opossum {

template <typename T>
const AllTypeVariant ValueColumn<T>::operator[](const size_t i) const {
  PerformanceWarning("operator[] used");
  return _values.at(i);
}

template <typename T>
void ValueColumn<T>::append(const AllTypeVariant& val) {
  _values.push_back(type_cast<T>(val));
}

template <typename T>
size_t ValueColumn<T>::size() const {
  return _values.size();
}

template <typename T>
const std::vector<T>& ValueColumn<T>::values() const {
  return _values;
}

EXPLICITLY_INSTANTIATE_COLUMN_TYPES(ValueColumn);

}  // namespace opossum
