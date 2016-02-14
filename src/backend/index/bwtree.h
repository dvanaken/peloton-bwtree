//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// BWTree.h
//
// Identification: src/backend/index/BWTree.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <unordered_map>
#include <vector>

#include "backend/common/types.h"

namespace peloton {
namespace index {

// Look up the stx btree interface for background.
// peloton/third_party/stx/btree.h
template <typename KeyType, typename ValueType, class KeyComparator>
class BWTree {
 public:

  // TODO: insert function

  // TODO: delete function

  // TODO: update function

  // TODO: search function

 private:
  class Node {

    // PIDs of all children
    std::vector<uint64_t> children_;

    // Level in the bw-tree, is a leaf node if level == 0
    uint32_t level_;

    // Temporary node link used during structure modifications
    uint64_t smo_link_;

    // Max key in this node
    KeyType max_fence_;

    // Returns true if this is a leaf node
    inline bool isLeafNode() const {
      return (level_ == 0);
    }
  };

  class LeafNode : public Node {

    // PID of next child
    uint64_t next_leaf_;

    // PID of previous child
    uint64_t prev_leaf_;

    // Pointers to all data stored in this leaf
    std::vector<ItemPointer> data_items_;
  };

  // PID of the root node
  uint64_t root_;

  // Maps node PIDs to memory locations
  std::unordered_map<uint64_t, void*> map_table_;

};

}  // End index namespace
}  // End peloton namespace
