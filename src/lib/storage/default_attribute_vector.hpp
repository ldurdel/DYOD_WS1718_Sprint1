#pragma once

#include <vector>

#include "base_attribute_vector.hpp"
#include "types.hpp"

namespace opossum {

// Stores entries with the same bit length as the data type behind ValueID.
class DefaultAttributeVector : BaseAttributeVector {
 public:
  explicit DefaultAttributeVector(size_t number_of_entries);

  ValueID get(const size_t i);

  void set(const size_t i, const ValueID value_id);

  size_t size();

  AttributeVectorWidth width();

 protected:
  std::vector<ValueID> _values;
};
}  // namespace opossum
