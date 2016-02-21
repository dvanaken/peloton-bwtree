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

template <typename KeyType, typename ValueType, typename KeyComparator, typename KeyEqualityChecker>
BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::BWTree() {
  root_ = 0;
  PID_counter_ = 1;
  allow_duplicate_ = true;

  InnerNode* root_base_page = new InnerNode();
  map_table_[root_] = root_base_page;

  // Can't do this here because we're using atomic inside vector
  // map_table_.resize(1000000);
}

template <typename KeyType, typename ValueType, typename KeyComparator, typename KeyEqualityChecker>
BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::BWTree(bool allow_duplicate) {
  root_ = 0;
  PID_counter_ = 1;
  allow_duplicate_ = allow_duplicate;

  InnerNode* root_base_page = new InnerNode();
  map_table_[root_] = root_base_page;

}

template <typename KeyType, typename ValueType, typename KeyComparator, typename KeyEqualityChecker>
bool BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Insert(const KeyType& key,
                                                                           const ValueType& data) {
  while (true) {
    Page* root_page = map_table_[root_];
    if (root_page->GetType() == INNER_NODE &&
        reinterpret_cast<InnerNode*>(root_page)->children_.size() == 0) {
      // InnerNode* root_base_page = reinterpret_cast<InnerNode*>(root_page);

      // Construct our first leaf node
      LeafNode* leaf_base_page = new LeafNode();
      std::vector<ItemPointer> locations;
      locations.push_back(data);
      leaf_base_page->data_items_.push_back(std::make_pair(key, locations));
      leaf_base_page->low_key_ = key;
      leaf_base_page->high_key_ = key;

      // Install the leaf node to mapping table
      PID leaf_PID = InstallNewMapping(leaf_base_page);

      // Construct the index term delta record
      IndexTermDelta* index_term_page = new IndexTermDelta(key, key, leaf_PID);
      index_term_page->SetDeltaNext(root_page);

      // If prepending the IndexTermDelta fails, we need to free the resource
      // and start over the insert again.
      if (map_table_[root_].compare_exchange_strong(root_page,
                                                    index_term_page)) {
        return true;
      } else {
        // TODO: Garbage collect leaf_base_page, index_term_page and leaf_PID.

        continue;
      }
    }
  }

  return false;
}

// Explicit template instantiation
template class BWTree<IntsKey<1>, ItemPointer, IntsComparator<1>,
IntsEqualityChecker<1> >;
template class BWTree<IntsKey<2>, ItemPointer, IntsComparator<2>,
IntsEqualityChecker<2> >;
template class BWTree<IntsKey<3>, ItemPointer, IntsComparator<3>,
IntsEqualityChecker<3> >;
template class BWTree<IntsKey<4>, ItemPointer, IntsComparator<4>,
IntsEqualityChecker<4> >;

template class BWTree<GenericKey<4>, ItemPointer, GenericComparator<4>,
GenericEqualityChecker<4> >;
template class BWTree<GenericKey<8>, ItemPointer, GenericComparator<8>,
GenericEqualityChecker<8> >;
template class BWTree<GenericKey<12>, ItemPointer, GenericComparator<12>,
GenericEqualityChecker<12> >;
template class BWTree<GenericKey<16>, ItemPointer, GenericComparator<16>,
GenericEqualityChecker<16> >;
template class BWTree<GenericKey<24>, ItemPointer, GenericComparator<24>,
GenericEqualityChecker<24> >;
template class BWTree<GenericKey<32>, ItemPointer, GenericComparator<32>,
GenericEqualityChecker<32> >;
template class BWTree<GenericKey<48>, ItemPointer, GenericComparator<48>,
GenericEqualityChecker<48> >;
template class BWTree<GenericKey<64>, ItemPointer, GenericComparator<64>,
GenericEqualityChecker<64> >;
template class BWTree<GenericKey<96>, ItemPointer, GenericComparator<96>,
GenericEqualityChecker<96> >;
template class BWTree<GenericKey<128>, ItemPointer, GenericComparator<128>,
GenericEqualityChecker<128> >;
template class BWTree<GenericKey<256>, ItemPointer, GenericComparator<256>,
GenericEqualityChecker<256> >;
template class BWTree<GenericKey<512>, ItemPointer, GenericComparator<512>,
GenericEqualityChecker<512> >;

template class BWTree<TupleKey, ItemPointer, TupleKeyComparator,
TupleKeyEqualityChecker>;

}  // End index namespace
}  // End peloton namespace
