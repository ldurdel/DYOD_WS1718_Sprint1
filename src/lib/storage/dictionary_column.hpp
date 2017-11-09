#pragma once

#include <algorithm>
#include <iterator>
#include <limits>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "all_type_variant.hpp"
#include "default_attribute_vector.hpp"
#include "fitted_attribute_vector.hpp"
#include "type_cast.hpp"
#include "types.hpp"

namespace opossum {

class BaseAttributeVector;
class BaseColumn;

// Even though ValueIDs do not have to use the full width of ValueID (uint32_t), this will also work for smaller ValueID
// types (uint8_t, uint16_t) since after a down-cast INVALID_VALUE_ID will look like their numeric_limit::max()
constexpr ValueID INVALID_VALUE_ID{std::numeric_limits<ValueID::base_type>::max()};

// Dictionary is a specific column type that stores all its values in a vector
template <typename T>
class DictionaryColumn : public BaseColumn {
 public:
  /**
   * Creates a Dictionary column from a given value column.
   */
  explicit DictionaryColumn(const std::shared_ptr<BaseColumn>& base_column) { _compress_values(base_column); }

  // SEMINAR INFORMATION: Since most of these methods depend on the template parameter, you will have to implement
  // the DictionaryColumn in this file. Replace the method signatures with actual implementations.

  // return the value at a certain position. If you want to write efficient operators, back off!
  const AllTypeVariant operator[](const size_t i) const override { return AllTypeVariant{get(i)}; }

  // return the value at a certain position.
  const T get(const size_t i) const { return _dictionary->at(_attribute_vector->get(i)); }

  // dictionary columns are immutable
  void append(const AllTypeVariant&) override { throw std::runtime_error("DictionaryColumns are immutable"); };

  // returns an underlying dictionary
  std::shared_ptr<const std::vector<T>> dictionary() const { return _dictionary; }

  // returns an underlying data structure
  std::shared_ptr<const BaseAttributeVector> attribute_vector() const {
    return std::dynamic_pointer_cast<const BaseAttributeVector>(_attribute_vector);
  }

  // return the value represented by a given ValueID
  const T& value_by_value_id(ValueID value_id) const { return _dictionary->at(value_id); }

  // returns the first value ID that refers to a value >= the search value
  // returns INVALID_VALUE_ID if all values are smaller than the search value
  ValueID lower_bound(T value) const {
    auto iterator = std::lower_bound(_dictionary->cbegin(), _dictionary->cend(), value);
    if (iterator == _dictionary->cend()) {
      return INVALID_VALUE_ID;
    }

    return ValueID{static_cast<ValueID::base_type>(iterator - _dictionary->cbegin())};
  }

  // same as lower_bound(T), but accepts an AllTypeVariant
  ValueID lower_bound(const AllTypeVariant& value) const { return lower_bound(type_cast<T>(value)); }

  // returns the first value ID that refers to a value > the search value
  // returns INVALID_VALUE_ID if all values are smaller than or equal to the search value
  ValueID upper_bound(T value) const {
    auto iterator = std::upper_bound(_dictionary->cbegin(), _dictionary->cend(), value);
    if (iterator == _dictionary->cend()) {
      return INVALID_VALUE_ID;
    }

    return ValueID{static_cast<ValueID::base_type>(iterator - _dictionary->cbegin())};
  }

  // same as upper_bound(T), but accepts an AllTypeVariant
  ValueID upper_bound(const AllTypeVariant& value) const { return upper_bound(type_cast<T>(value)); }

  // return the number of unique_values (dictionary entries)
  size_t unique_values_count() const { return _dictionary->size(); }

  // return the number of entries
  size_t size() const override { return _attribute_vector->size(); }

 private:
  // puts all values in a map to deduplicate, then reads out the map to build up values
  void _compress_values(const std::shared_ptr<BaseColumn>& base_column) {
    // ToDo: use correct smart pointer type
    const std::shared_ptr<ValueColumn<T>>& p_column = std::dynamic_pointer_cast<ValueColumn<T>>(base_column);
    Assert(p_column, "base_column has invalid type that does not match <T>");
    const ValueColumn<T>& column = *p_column;

    // First step: iterate over all AllTypeVariant values and deduplicate
    std::set<T> unique_values;

    // ToDo: std::copy oder iterator oder irgendwas optimiertes
    for (auto index = 0u; index < column.size(); ++index) {
      unique_values.insert(type_cast<T>(column[index]));
    }

    // Create (sorted) dictionary
    _dictionary = std::make_shared<std::vector<T>>(unique_values.size());

    // (Set iterates in a sorted manner)
    ValueID counter{0};
    for (const auto& unique_value : unique_values) {
      (*_dictionary)[counter++] = unique_value;
    }

    // ToDo: Implement logic that chooses the best AttributeVector class based on the number of different values
    if (_dictionary->size() <= std::numeric_limits<uint8_t>::max()) {
      _attribute_vector = std::dynamic_pointer_cast<BaseAttributeVector>(
          std::make_shared<FittedAttributeVector<uint8_t>>(column.size()));
    } else if (_dictionary->size() <= std::numeric_limits<uint16_t>::max()) {
      _attribute_vector = std::dynamic_pointer_cast<BaseAttributeVector>(
          std::make_shared<FittedAttributeVector<uint16_t>>(column.size()));
    } else {
      _attribute_vector = std::dynamic_pointer_cast<BaseAttributeVector>(
          std::make_shared<FittedAttributeVector<uint32_t>>(column.size()));
    }

    // Now, fill the attribute vector (for each value, perform binary search to find the dictionary index)
    // (Since we created _dictionary by using a set, we can be sure that each value exists exactly once,
    //  and thus we do not need to perform checks on the binary search result)
    for (auto index = 0u; index < column.size(); ++index) {
      auto iterator = std::lower_bound(_dictionary->cbegin(), _dictionary->cend(), type_cast<T>(column[index]));
      _attribute_vector->set(index, ValueID{static_cast<ValueID::base_type>(iterator - _dictionary->cbegin())});
    }
  }

 protected:
  std::shared_ptr<std::vector<T>> _dictionary;
  std::shared_ptr<BaseAttributeVector> _attribute_vector;
};

}  // namespace opossum
