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
  using PID = std::uint64_t;
  constexpr PID NullPID = std::numeric_limits<PID>::max();

 public:
  enum PageType {
    // For leaf nodes.
    LEAF_NODE,
    // Because we only have pointers of data items, we actually don't need the
    // modify operation.
    // There's no update operation in bwtree_index.h, either.
    INSERT_DELTA,
    // MODIFY_DELTA,
    DELETE_DELTA,

    // For inner nodes.
    INNER_NODE,
    SPLIT_DELTA,
    INDEX_TERM_DELTA,
    REMOVE_NODE_DELTA,
    NODE_MERGE_DELTA

    // It seems that INDEX_TERM_DELETE_DELTA is the same as INDEX_TERM_DELTA,
    // so currently I don't use this type.
    // INDEX_TERM_DELETE_DELTA
  };

  // TODO: insert function

  // TODO: delete function

  // TODO: update function

  // TODO: search function

 private:
  class Page {
   protected:
    PageType type_;

   public:
    Page(PageType type) : type_(type) {}

    const inline PageType& GetType() const { return type_; }
  };

  class InnerNode : public Page {
   public:
    // Separator keys and links for the children
    // The key in the pair is the high_key_ of the child.
    std::vector<std::pair<KeyType, PID> > children_;

    // Node link used during structure modifications
    PID side_link_;

    // Min key in this node
    KeyType low_key_;

    // Max key in this node
    KeyType high_key_;

    InnerNode(): Page(INNER_NODE), side_link_(NullPID) {}
  };

  class LeafNode : public Page {
   public:
    // PID of next child
    PID next_leaf_;

    // PID of previous child
    PID prev_leaf_;

    // Keys and pointers to all data stored in this leaf
    std::vector<std::pair<KeyType, ItemPointer> > data_items_;

    // Temporary node link used during structure modifications
    PID side_link_;

    // Min key in this node
    KeyType low_key_;

    // Max key in this node
    KeyType high_key_;

    LeafNode(): Page(LEAF_NODE), side_link_(NullPID), next_leaf_(NullPID), prev_leaf_(NullPID) {}
  };

  class SplitDelta : public Page {
   public:
    // Invalidate all records within the node greater than separator_
    KeyType separator_;

    PID side_link_;

    SplitDelta(KeyType separator_key, PID new_sibling)
        : Page(SPLIT_DELTA),
          separator_(separator_key),
          side_link_(new_sibling) {}
  };

  // Direct to records greater than low_separator_ and less than or equal to
  // high_separator_
  class IndexTermDelta : public Page {
   public:
    KeyType low_separator_;
    KeyType high_separator_;

    PID side_link_;

    IndexTermDelta(KeyType low_separator_key, KeyType high_separator_key,
                   PID new_sibling)
        : Page(INDEX_TERM_DELTA),
          low_separator_(low_separator_key),
          high_separator_(high_separator_key),
          side_link_(new_sibling) {}
  };

  // Stops all further use of node
  class RemoveNodeDelta : public Page {
   public:
    RemoveNodeDelta() : Page(REMOVE_NODE_DELTA) {}
  };

  class NodeMergeDelta : public Page {
   public:
    // Direct all records greater than separator_ to physical_link_
    KeyType separator_;

    void* physical_link_;

    NodeMergeDelta(KeyType separator_key, void* new_sibling)
        : Page(NODE_MERGE_DELTA),
          separator_(separator_key),
          physical_link_(new_sibling) {}
  };

  class InsertDelta : public Page {
   public:
    KeyType key_;

    ItemPointer location_;

    InsertDelta(KeyType key, ItemPointer location)
        : Page(INSERT_DELTA), key_(key), location_(location) {}
  };

  class DeleteDelta : public Page {
   public:
    KeyType key_;

    ItemPointer location_;

    DeleteDelta(KeyType key, ItemPointer location)
        : Page(DELETE_DELTA), key_(key), location_(location) {}
  };

  // PID of the root node
  PID root_;

  // Maps node PIDs to memory locations
  std::unordered_map<PID, void*> map_table_;
};

}  // End index namespace
}  // End peloton namespace
