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

#include <condition_variable>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <stack>
#include <map>
#include <set>
#include <atomic>

#include "backend/common/types.h"
#include "backend/common/logger.h"
#include "backend/common/platform.h"
#include "backend/index/index_key.h"

#define CONSOLIDATE_THRESHOLD 3
#define SPLIT_SIZE 4
#define MERGE_SIZE 4
#define EPOCH_INTERVAL_MS 40  // in milliseconds

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
  ~BWTree();

  // TODO (dana): I can't get this to compile after I add allow_duplicate as a
  // param
  // BWTree(const KeyComparator& comparator, const KeyEqualityChecker& equals,
  //  bool allow_duplicate);

  // BWTree();
  // BWTree(bool allow_duplicate);

  // Insert function
  bool Insert(const KeyType& key, const ValueType& data);

  // Delete function
  bool Delete(const KeyType& key, const ValueType& data);

  // Search functions
  std::vector<ValueType> SearchKey(const KeyType& key);
  std::map<KeyType, std::vector<ValueType>, KeyComparator> SearchAllKeys();

  bool Cleanup();

  size_t GetMemoryFootprint();

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

    virtual ~Page() { };

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

    // Min key in this node - it's actually not in this node right ? (aaron)
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
    PID merged_into_;
    RemoveNodeDelta(PID merged_into)
        : Page(REMOVE_NODE_DELTA), merged_into_(merged_into) {}
  };

  class NodeMergeDelta : public Page {
   public:
    // Direct all records greater than separator_ to physical_link_
    KeyType separator_;
    bool absolute_max_;
    Page* physical_link_;

    NodeMergeDelta(KeyType separator_key, bool absolute_max, Page* new_sibling)
        : Page(NODE_MERGE_DELTA),
          separator_(separator_key),
          absolute_max_(absolute_max),
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

  bool Split_Operation(Page* consolidated_page, std::stack<PID>& pages_visited,
                       PID orig_pid);
  bool complete_the_split(PID side_link, std::stack<PID>& pages_visited);

  void Merge_Operation(Page* consolidated_page, std::stack<PID>& pages_visited,
                       PID orig_pid);
  PID find_left_sibling(std::stack<PID>& pages_visited, KeyType merge_key);
  bool complete_the_merge(RemoveNodeDelta* remove_node,
                          std::stack<PID>& pages_visited);
  bool is_merge_installed(Page* page_merging_into, Page* compare_to,
                          KeyType high_key);

  inline PID InstallNewMapping(Page* new_page) {
    PID new_slot = PID_counter_++;

    if (PID_counter_ >= map_table_.size()) {
      LOG_ERROR("FATAL: Not enough space in mapping table!");
      return NullPID;
    }

    map_table_[new_slot] = new_page;
    return new_slot;
  }

  inline bool CheckConsolidate(PID page_PID) {
    Page* current_page = map_table_[page_PID];
    int page_length = 0;

    // Traverse the delta chain to get length. We count NODE_MERGE_DELTA as 2
    // length because we don't know the length of it. But if the merged node
    // consolidated successfully before merging, then it should have only one
    // extra length.
    while (current_page != nullptr) {
      if (current_page->GetType() == NODE_MERGE_DELTA) page_length++;
      page_length++;
      current_page = current_page->GetDeltaNext();
    }

    if (page_length > CONSOLIDATE_THRESHOLD)
      return true;
    else
      return false;
  }

  inline Page* Consolidate(PID page_PID) {
    std::vector<ValueType> result;
    Page* current_page = map_table_[page_PID];

    // while (current_page != nullptr)
    //  current_page = current_page->GetDeltaNext();

    std::map<KeyType, PID, KeyComparator> key_pointers(comparator_);
    std::map<KeyType, std::vector<ValueType>, KeyComparator> key_locations(
        comparator_);
    std::map<KeyType, std::pair<KeyType, PID>, KeyComparator> index_term_ranges(
        comparator_);

    // Indicate whether we have met absolute min/max along consolidation
    bool absolute_min = false;
    bool absolute_max = false;
    bool is_leaf;
    // if (current_page->GetType() == INNER_NODE)
    // else

    // Finally I found out a way to use set here... so we dont't need this
    // VisitedChecker<KeyType, KeyEqualityChecker> visited_keys;

    // std::stack<Page*> physical_links;
    // std::stack<bool> split_indicators;
    // std::stack<KeyType> split_separators;
    bool split_indicator = false;
    KeyType split_separator = KeyType();
    bool merge_indicator = false;
    PID side_link = -1;

    KeyType leaf_low_key;
    PID next_leaf = -1;
    PID prev_leaf = -1;

    Page* merge_link = nullptr;

    current_page = map_table_[page_PID];
    bool stop = false;
    while (!stop) {
      switch (current_page->GetType()) {
        case INNER_NODE: {
          InnerNode* inner_node = reinterpret_cast<InnerNode*>(current_page);

          KeyType last_key = inner_node->low_key_;
          bool first_child = true;
          for (const auto& child : inner_node->children_) {
            // Check whether a child is already contained in an index_term_delta
            bool insert = true;
            bool first_element = true;
            for (auto& key_value : index_term_ranges) {
              if (((first_element && absolute_min) ||
                   (!(first_child && inner_node->absolute_min_) &&
                    reverse_comparator_(key_value.first, last_key) <= 0)) &&
                  reverse_comparator_(key_value.second.first, last_key) > 0) {
                insert = false;
                break;
              }
              first_element = false;
            }

            // Check whether the child has been split out
            if (split_indicator &&
                reverse_comparator_(split_separator, child.first) < 0)
              insert = false;

            if (insert) {
              index_term_ranges.emplace(last_key, child);
            }

            last_key = child.first;
            first_child = false;
          }

          is_leaf = false;
          if (inner_node->absolute_min_) absolute_min = true;
          if (inner_node->absolute_max_) absolute_max = true;

          if (merge_link != nullptr) {
            current_page = merge_link;
            merge_link = nullptr;
          } else {
            if (!split_indicator) side_link = inner_node->side_link_;
            stop = true;
          }

          continue;
        }
        case INDEX_TERM_DELTA: {
          IndexTermDelta* idx_delta =
              reinterpret_cast<IndexTermDelta*>(current_page);
          LOG_DEBUG("Current page: %p", current_page);

          LOG_DEBUG("Consolidate INDEX_TERM_DELTA with low key: %s high key: %s",
                idx_delta->low_separator_.GetTupleForComparison(key_tuple_schema).GetInfo().c_str(),
                idx_delta->high_separator_.GetTupleForComparison(key_tuple_schema).GetInfo().c_str());

          bool first_element = true;
          bool end_case = false;
          for (auto& key_value : index_term_ranges) {
            if ((((first_element && absolute_min) ||
                  (!idx_delta->absolute_min_ &&
                   reverse_comparator_(key_value.first,
                                       idx_delta->low_separator_) <= 0))) &&
                reverse_comparator_(key_value.second.first,
                                    idx_delta->low_separator_) > 0) {
              current_page = current_page->GetDeltaNext();
              end_case = true;
              break;
            }

            first_element = false;
          }

          if (end_case)
            continue;



          if (idx_delta->absolute_min_) absolute_min = true;
          if (idx_delta->absolute_max_) absolute_max = true;

          index_term_ranges.emplace(idx_delta->low_separator_,
                                    std::make_pair(idx_delta->high_separator_,
                                                   idx_delta->side_link_));
          // Keep traversing the delta chain.
          current_page = current_page->GetDeltaNext();
          LOG_DEBUG("Next page: %p", current_page);
          continue;
        }
        case SPLIT_DELTA: {
          // make sure we consolidate before split
          __attribute__((unused)) PageType next_page_type =
              current_page->GetDeltaNext()->GetType();
          assert(next_page_type == INNER_NODE || next_page_type == LEAF_NODE);

          SplitDelta* split_delta = reinterpret_cast<SplitDelta*>(current_page);
          split_indicator = true;
          split_separator = split_delta->separator_;
          side_link = split_delta->side_link_;

          current_page = current_page->GetDeltaNext();
          break;
        }
        case REMOVE_NODE_DELTA: {
          break;
        }
        case NODE_MERGE_DELTA: {
          __attribute__((unused)) PageType next_page_type =
              current_page->GetDeltaNext()->GetType();
          assert(next_page_type == INNER_NODE || next_page_type == LEAF_NODE);
          NodeMergeDelta* merge_delta =
              reinterpret_cast<NodeMergeDelta*>(current_page);

          next_page_type = merge_delta->physical_link_->GetType();
          assert(next_page_type == INNER_NODE || next_page_type == LEAF_NODE);

          merge_link = merge_delta->physical_link_;
          merge_indicator = true;

          current_page = current_page->GetDeltaNext();
          break;
        }
        case LEAF_NODE: {
          LeafNode* leaf = reinterpret_cast<LeafNode*>(current_page);

          // Traverse all data item in the leaf. If we already visited before,
          // then skip.
          std::vector<ValueType> data_items;
          for (const auto& key_values : leaf->data_items_) {
            if (split_indicator &&
                reverse_comparator_(split_separator, key_values.first) < 0)
              break;
            key_locations.insert(key_values);
          }

          is_leaf = true;
          if (leaf->absolute_min_) absolute_min = true;
          if (leaf->absolute_max_) absolute_max = true;

          if (merge_link != nullptr) {
            current_page = merge_link;
            merge_link = nullptr;
            leaf_low_key = leaf->low_key_;
            prev_leaf = leaf->prev_leaf_;
          } else {
            if (!merge_indicator) {
              leaf_low_key = leaf->low_key_;
              prev_leaf = leaf->prev_leaf_;
            }
            next_leaf = leaf->next_leaf_;

            if (!split_indicator) side_link = leaf->side_link_;
            stop = true;
          }
          continue;
        }
        case MODIFY_DELTA: {
          ModifyDelta* mod_delta = reinterpret_cast<ModifyDelta*>(current_page);
          key_locations.insert(
              std::make_pair(mod_delta->key_, mod_delta->locations_));

          current_page = current_page->GetDeltaNext();
          continue;
        }
        default:
          // throw IndexException("Unrecognized page type\n");
          break;
      }
    }

    if (is_leaf) {
      LeafNode* new_leaf = new LeafNode();
      new_leaf->low_key_ = leaf_low_key;
      if (!key_locations.empty())
        new_leaf->high_key_ = key_locations.rbegin()->first;
      else {
        LOG_DEBUG("We meet an empty leaf node!");
      }
      new_leaf->absolute_min_ = absolute_min;
      if (!split_indicator) new_leaf->absolute_max_ = absolute_max;

      if (split_indicator)
        new_leaf->next_leaf_ = side_link;
      else
        new_leaf->next_leaf_ = next_leaf;

      new_leaf->prev_leaf_ = prev_leaf;

      new_leaf->side_link_ = side_link;

      LOG_DEBUG("Consolidate leaf item:");
      // KeyType old_key;
      for (auto& key_location : key_locations) {
        if (key_location.second.size())
          new_leaf->data_items_.push_back(key_location);
        // LOG_DEBUG("%d", comparator_(old_key, key_location.first));
        // old_key = key_location.first;
        LOG_DEBUG("one entry with size: %ld", key_location.second.size());
      }
      LOG_DEBUG("Consolidate leaf item end.");
      return new_leaf;
    } else {
      InnerNode* new_inner = new InnerNode();
      if (!index_term_ranges.empty()) {
        new_inner->low_key_ = index_term_ranges.begin()->first;
        new_inner->high_key_ = index_term_ranges.rbegin()->second.first;
      } else {
        LOG_DEBUG("We meet an empty inner node!");
      }
      new_inner->absolute_min_ = absolute_min;
      if (!split_indicator) new_inner->absolute_max_ = absolute_max;

      new_inner->side_link_ = side_link;

      if (!index_term_ranges.empty()) {
        LOG_DEBUG("Consolidate inner item:");
        bool first_child = true;
        PID last_PID = index_term_ranges.begin()->second.second;
        for (auto& kv : index_term_ranges) {
          if (first_child) {
            first_child = false;
            continue;
          }
          new_inner->children_.push_back(std::make_pair(kv.first, last_PID));
          last_PID = kv.second.second;
          __attribute__((unused)) KeyType tmp_key = kv.first;
          LOG_DEBUG("one entry with high key : %s", tmp_key.GetTupleForComparison(key_tuple_schema).GetInfo().c_str());
        }
        new_inner->children_.push_back(
            std::make_pair(index_term_ranges.rbegin()->second.first, last_PID));
        LOG_DEBUG("one entry with high key : %s", index_term_ranges.rbegin()->second.first.GetTupleForComparison(key_tuple_schema).GetInfo().c_str());
        LOG_DEBUG("Consolidate inner item end.");
      }
      return new_inner;
    }
  }

  inline void FreeDeltaChain(Page* page) {
    // Actually we can't free stale delta chain here, because other threads
    // might be reading it.
    Page* free_page;
    while (page != nullptr) {
      free_page = page;
      page = page->GetDeltaNext();
      delete free_page;
    }
  }

  // ***** Member variables

  class ReverseComparator {
   public:
    KeyComparator comparator_;

    ReverseComparator(KeyComparator a) : comparator_(a) {}

    inline int operator()(const KeyType& lhs, const KeyType& rhs) const {
      if (comparator_(lhs, rhs)) return -1;

      if (comparator_(rhs, lhs)) return 1;

      return 0;
    }
  };

  void RunEpochManager();

  inline void Start() {
    epoch_manager_ = std::thread(&BWTree::RunEpochManager, this);
  }

  uint64_t RegisterWorker();

  void DeregisterWorker(uint64_t worker_epoch);

  void DeallocatePage(Page *page);

  size_t GetPageSize(Page *page);

  // PID of the root node
  PID root_;

  // Counter for the current PID
  std::atomic<PID> PID_counter_;

  // Maps node PIDs to memory locations
  // TODO: Because std::atomic is not CopyInsertable, we have to use a fix
  // size
  // here. Need some sort of garbage collection later
  std::vector<std::atomic<Page*>> map_table_{1000000};

  // True if duplicate keys are permitted
  bool allow_duplicate_;

  // Key comparison function object
  KeyComparator comparator_;

  // Key equality function object
  KeyEqualityChecker equals_;

  ReverseComparator reverse_comparator_;

  // Value equality function object
  ValueEqualityChecker value_equals_;

  std::atomic_bool finished_;

  std::thread epoch_manager_;

  std::atomic_ullong epoch_;

  std::unordered_map<uint64_t, std::atomic_ullong> active_threads_map_;

  std::unordered_map<uint64_t, std::vector<Page*>> epoch_garbage_;

  std::condition_variable exec_finished_;

  // TODO: synch helper for debug
  RWLock cleanup_lock;
  RWLock dealloc_lock;

  // Used for debug
  catalog::Schema *key_tuple_schema;
};

}  // End index namespace
}  // End peloton namespace
