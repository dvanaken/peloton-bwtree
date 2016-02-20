//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// bwtree.cpp
//
// Identification: src/backend/index/bwtree.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/index/bwtree.h"
#include "backend/index/index_key.h"

namespace peloton {
namespace index {

template <typename KeyType, typename ValueType, class KeyComparator>
bool BWTree<KeyType, ValueType, KeyComparator>::insert(const KeyType& key,
                                                       const ValueType& data) {
  insert(key, data);
  return false;
}

// Explicit template instantiation
template class BWTree<IntsKey<1>, ItemPointer, IntsComparator<1> >;
template class BWTree<IntsKey<2>, ItemPointer, IntsComparator<2> >;
template class BWTree<IntsKey<3>, ItemPointer, IntsComparator<3> >;
template class BWTree<IntsKey<4>, ItemPointer, IntsComparator<4> >;

template class BWTree<GenericKey<4>, ItemPointer, GenericComparator<4> >;
template class BWTree<GenericKey<8>, ItemPointer, GenericComparator<8> >;
template class BWTree<GenericKey<12>, ItemPointer, GenericComparator<12> >;
template class BWTree<GenericKey<16>, ItemPointer, GenericComparator<16> >;
template class BWTree<GenericKey<24>, ItemPointer, GenericComparator<24> >;
template class BWTree<GenericKey<32>, ItemPointer, GenericComparator<32> >;
template class BWTree<GenericKey<48>, ItemPointer, GenericComparator<48> >;
template class BWTree<GenericKey<64>, ItemPointer, GenericComparator<64> >;
template class BWTree<GenericKey<96>, ItemPointer, GenericComparator<96> >;
template class BWTree<GenericKey<128>, ItemPointer, GenericComparator<128> >;
template class BWTree<GenericKey<256>, ItemPointer, GenericComparator<256> >;
template class BWTree<GenericKey<512>, ItemPointer, GenericComparator<512> >;

template class BWTree<TupleKey, ItemPointer, TupleKeyComparator>;

}  // End index namespace
}  // End peloton namespace
