#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base_column.hpp"

namespace opossum {

// ValueColumn is a specific column type that stores all its values in a vector
template <typename T>
class ValueColumn : public BaseColumn {
 public:
  // return the value at a certain position. If you want to write efficient operators, back off!
  const AllTypeVariant operator[](const size_t i) const override;

  // add a value to the end
  void append(const AllTypeVariant& val) override;

  // return the number of entries
  size_t size() const override;

 protected:
  std::vector<T> _values;
};

}  // namespace opossum
