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
#include <atomic>

#include "backend/common/types.h"
#include "backend/common/logger.h"

namespace peloton {
namespace index {

struct ItemPointerEqualityChecker {
  bool operator()(const ItemPointer& l, const ItemPointer& r) const {
    if (l.block == r.block && l.offset == r.offset) return true;
    return false;
  }
};

// Look up the stx btree interface for background.
// peloton/third_party/stx/btree.h
template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker,
          class ValueEqualityChecker = ItemPointerEqualityChecker>
class BWTree {
  using PID = std::uint64_t;
  static constexpr PID NullPID = std::numeric_limits<PID>::max();

 public:
  BWTree(const KeyComparator& comparator, const KeyEqualityChecker& equals);

  // TODO (dana): I can't get this to compile after I add allow_duplicate as a
  // param
  // BWTree(const KeyComparator& comparator, const KeyEqualityChecker& equals,
  //  bool allow_duplicate);

  // BWTree();
  // BWTree(bool allow_duplicate);

  // TODO: insert function
  bool Insert(const KeyType& key, const ValueType& data);

  // TODO: delete function

  // TODO: update function

  // TODO: search function

 private:
  // ***** Different types of page records

  enum PageType {
    // For leaf nodes.
    LEAF_NODE,
    // Combine the following three to one for convenience.
    // INSERT_DELTA,
    MODIFY_DELTA,
    // DELETE_DELTA,

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

  class Page {
   protected:
    PageType type_;
    Page* delta_next_;

   public:
    Page(PageType type) : type_(type) { delta_next_ = nullptr; }

    const inline PageType& GetType() const { return type_; }

    inline void SetDeltaNext(Page* next) { delta_next_ = next; }

    inline Page* GetDeltaNext() { return delta_next_; }
  };

  // A key belongs to an InnerNode if at least one of the following exists:
  // (1) absolute_min_ == true && absolute_max_ == true
  // (2) absolute_min_ == true && key <= children_.rbegin()->first
  // (3) absolute_max == true && key > children_.begin()->first
  // (4) children_.begin()->first < key <= children_.begin()->first
  class InnerNode : public Page {
   public:
    // Separator keys and links for the children
    // The key in the pair is the high_key_ of the child.
    std::vector<std::pair<KeyType, PID>> children_;

    // Node link used during structure modifications
    PID side_link_;

    // Min key in this node
    KeyType low_key_;

    // Max key in this node
    KeyType high_key_;

    // Indicate whether the range contains min KeyType
    bool absolute_min_;

    // Indicate whether the range contains max KeyType
    bool absolute_max_;

    InnerNode()
        : Page(INNER_NODE),
          side_link_(NullPID),
          absolute_min_(false),
          absolute_max_(false) {}
  };

  // A key belongs to an LeafNode if at least one of the following exists:
  // (1) absolute_min_ == true && absolute_max_ == true
  // (2) absolute_min_ == true && key <= data_items.rbegin()->first
  // (3) absolute_max == true && key > data_items_.begin()->first
  // (4) data_items_.begin()->first < key <= data_items_.rbegin()->first
  class LeafNode : public Page {
   public:
    // PID of next child
    PID next_leaf_;

    // PID of previous child
    PID prev_leaf_;

    // Keys and pointers to all data stored in this leaf
    std::vector<std::pair<KeyType, std::vector<ValueType>>> data_items_;

    // Temporary node link used during structure modifications
    PID side_link_;

    // Min key in this node
    KeyType low_key_;

    // Max key in this node
    KeyType high_key_;

    // Indicate whether the rage contains min KeyType
    bool absolute_min_;

    // Indicate whether the rage contains max KeyType
    bool absolute_max_;

    LeafNode()
        : Page(LEAF_NODE),
          next_leaf_(NullPID),
          prev_leaf_(NullPID),
          side_link_(NullPID),
          absolute_min_(false),
          absolute_max_(false) {}
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
  // high_separator_.
  // A key belongs to an IndexTermDelta if at least one of the following exists:
  // (1) absolute_min_ == true && absolute_max_ == true
  // (2) absolute_min_ == true && key <= high_separator_
  // (3) absolute_max == true && key > low_separator_
  // (4) low_separator < key <= high_separator_
  class IndexTermDelta : public Page {
   public:
    KeyType low_separator_;
    KeyType high_separator_;

    // Indicate whether the rage contains min KeyType
    bool absolute_min_;
    // Indicate whether the rage contains max KeyType
    bool absolute_max_;

    PID side_link_;

    IndexTermDelta(KeyType low_separator_key, KeyType high_separator_key,
                   PID new_sibling)
        : Page(INDEX_TERM_DELTA),
          low_separator_(low_separator_key),
          high_separator_(high_separator_key),
          side_link_(new_sibling) {
      absolute_min_ = false;
      absolute_max_ = false;
    }
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

    Page* physical_link_;

    NodeMergeDelta(KeyType separator_key, Page* new_sibling)
        : Page(NODE_MERGE_DELTA),
          separator_(separator_key),
          physical_link_(new_sibling) {}
  };

  class ModifyDelta : public Page {
   public:
    KeyType key_;

    std::vector<ValueType> locations_;

    ModifyDelta(KeyType key, const std::vector<ValueType>& location)
        : Page(MODIFY_DELTA), key_(key), locations_(location) {}
  };

  // ***** Functions for internal usage

  inline PID InstallNewMapping(Page* new_page) {
    PID new_slot = PID_counter_++;

    if (PID_counter_ >= map_table_.size()) {
      LOG_ERROR("FATAL: Not enough space in mapping table!");
      return NullPID;
    }

    map_table_[new_slot] = new_page;
    return new_slot;
  }

  // ***** Member variables

  // PID of the root node
  PID root_;

  // Counter for the current PID
  std::atomic<PID> PID_counter_;

  // Maps node PIDs to memory locations
  // TODO: Because std::atomic is not CopyInsertable, we have to use a fix size
  // here. Need some sort of garbage collection later
  std::vector<std::atomic<Page*>> map_table_{1000000};

  // True if duplicate keys are permitted
  bool allow_duplicate_;

  // Key comparison function object
  KeyComparator comparator_;

  // Key equality function object
  KeyEqualityChecker equals_;

  // Value equality function object
  ValueEqualityChecker value_equals_;
};


}  // End index namespace
}  // End peloton namespace
