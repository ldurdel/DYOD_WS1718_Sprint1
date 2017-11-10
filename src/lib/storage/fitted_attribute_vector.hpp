#pragma once

#include <cstdint>
#include <limits>
#include <type_traits>
#include <vector>

#include "base_attribute_vector.hpp"
#include "types.hpp"

namespace opossum {

// Store entries with bit length determined by template parameter
template <typename T, typename = std::enable_if_t<std::is_integral<T>::value>>
class FittedAttributeVector : public BaseAttributeVector {
 public:
  explicit FittedAttributeVector(size_t number_of_entries) : _values(number_of_entries) {
    Assert(number_of_entries < std::numeric_limits<T>::max(), "Number of entries too large for underlying type");
  }

  ValueID get(const size_t i) const override { return ValueID{_values.at(i)}; };

  void set(const size_t i, const ValueID value_id) override {
    Assert(i < _values.size(), "Index out of bounds");
    _values[i] = value_id;
  };

  size_t size() const override { return _values.size(); };

  AttributeVectorWidth width() const override { return sizeof(T); };

 protected:
  std::vector<T> _values;
};
}  // namespace opossum
