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

#include <stack>

#include "backend/index/bwtree.h"
#include "backend/index/index_key.h"

namespace peloton {
namespace index {

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker, class ValueEqualityChecker>
BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker,
       ValueEqualityChecker>::BWTree(const KeyComparator& comparator,
                                     const KeyEqualityChecker& equals)
    : comparator_(comparator), equals_(equals) {
  root_ = 0;
  PID_counter_ = 1;
  allow_duplicate_ = true;

  InnerNode* root_base_page = new InnerNode();
  map_table_[root_] = root_base_page;

  // Can't do this here because we're using atomic inside vector
  // map_table_.resize(1000000);
}

// TODO (dana): can't get this to compile after I add the allow_duplicate param
// template <typename KeyType, typename ValueType, class KeyComparator, class
// KeyEqualityChecker>
// BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::BWTree(
//     const KeyComparator& comparator,
//     const KeyEqualityChecker& equals,
//     bool allow_duplicate)
//       : comparator_(comparator), equals_(equals),
//       allow_duplicate_(allow_duplicate) {
//   root_ = 0;
//   PID_counter_ = 1;
//   allow_duplicate_ = allow_duplicate;
//
//   InnerNode* root_base_page = new InnerNode();
//   map_table_[root_] = root_base_page;
//
// }

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker, class ValueEqualityChecker>
bool BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker,
            ValueEqualityChecker>::Insert(const KeyType& key,
                                          const ValueType& data) {
  LOG_DEBUG("Trying new insert");
  while (true) {
    Page* root_page = map_table_[root_];
    if (root_page->GetType() == INNER_NODE &&
        reinterpret_cast<InnerNode*>(root_page)->children_.size() == 0) {
      // InnerNode* root_base_page = reinterpret_cast<InnerNode*>(root_page);
      LOG_DEBUG("Constructing root page");

      // Construct our first leaf node
      LeafNode* leaf_base_page = new LeafNode();
      std::vector<ValueType> locations;
      locations.push_back(data);
      leaf_base_page->data_items_.push_back(std::make_pair(key, locations));
      leaf_base_page->low_key_ = key;
      leaf_base_page->high_key_ = key;
      leaf_base_page->absolute_min_ = true;
      leaf_base_page->absolute_max_ = true;

      // Install the leaf node to mapping table
      PID leaf_PID = InstallNewMapping(leaf_base_page);

      // Construct the index term delta record
      IndexTermDelta* index_term_page = new IndexTermDelta(key, key, leaf_PID);
      index_term_page->absolute_max_ = true;
      index_term_page->absolute_min_ = true;
      index_term_page->SetDeltaNext(root_page);

      // If prepending the IndexTermDelta fails, we need to free the resource
      // and start over the insert again.
      if (map_table_[root_].compare_exchange_strong(root_page,
                                                    index_term_page)) {
        LOG_DEBUG("CAS of first index_term_page successfull");
        return true;
      } else {
        // TODO: Garbage collect leaf_base_page, index_term_page and leaf_PID.

        continue;
      }
    } else {
      std::stack<PID> pages_visited;  // To remember parent nodes traversed on
                                      // our search path
      Page* current_page = root_page;
      PID current_PID = root_;
      Page* head_of_delta = current_page;
      bool attempt_insert = true;
      while (attempt_insert) {
        switch (current_page->GetType()) {
          case INNER_NODE: {
            InnerNode* inner_node = reinterpret_cast<InnerNode*>(current_page);
            assert(inner_node->absolute_min_ ||
                   comparator_(key, inner_node->low_key_) > 0);
            assert(inner_node->absolute_max_ ||
                   comparator_(key, inner_node->high_key_) <= 0);

            // TODO: use binary search here instead
            bool found_child = false;  // For debug only, should remove later
            for (const auto& child : inner_node->children_) {
              if (comparator_(key, child.first) <= 0) {
                // We need to go to this child next
                current_PID = child.second;
                current_page = map_table_[current_PID];
                head_of_delta = current_page;
                found_child = true;
                break;
              }
            }
            // We should always find a child to visit next
            if (!found_child)
              // compiler complaining
              assert(found_child);
            continue;
          }
          case INDEX_TERM_DELTA: {
            IndexTermDelta* idx_delta =
                reinterpret_cast<IndexTermDelta*>(current_page);
            // If this key is > low_separator_ and <= high_separator_ then this
            // is the child we
            // must visit next
            if ((idx_delta->absolute_min_ ||
                 comparator_(key, idx_delta->low_separator_) > 0) &&
                (idx_delta->absolute_max_ ||
                 comparator_(key, idx_delta->high_separator_) <= 0)) {
              // Follow the side link to this child
              current_PID = idx_delta->side_link_;
              current_page = map_table_[current_PID];
              head_of_delta = current_page;
              LOG_DEBUG("Found INDEX_TERM_DELTA to insert into");
            } else {
              // This key does not fall within the boundary represented by this
              // index term
              // delta record. Keep traversing the delta chain.
              current_page = current_page->GetDeltaNext();
            }
            continue;
          }
          case SPLIT_DELTA: {
            // TODO (dana)
            break;
          }
          case REMOVE_NODE_DELTA: {
            // TODO (dana)
            break;
          }
          case NODE_MERGE_DELTA: {
            // TODO (dana)
            break;
          }
          case LEAF_NODE: {
            __attribute__((unused)) LeafNode* leaf =
                reinterpret_cast<LeafNode*>(current_page);
            bool inserted;     // Whether this <key, value> pair is inserted
            bool install_new;  // Whether we need to install a new MODIFY_DELTA

            // TODO: this lookup should be binary search
            // Check if the given key is already located in this leaf
            bool found_key = false;
            std::vector<ValueType> data_items;
            for (const auto& key_values : leaf->data_items_) {
              if (equals_(key, key_values.first)) {
                found_key = true;
                // TODO: refactor common code in LEAF_NODE and MODIFY_DELTA
                // The key exists
                if (!allow_duplicate_) {
                  // No duplicates allowed so insert fails
                  inserted = false;
                  install_new = false;
                } else {
                  bool found_value = false;
                  for (const auto& value : key_values.second) {
                    if (value_equals_(value, data)) {
                      found_value = true;
                      break;
                    }
                  }
                  if (found_value) {
                    // TODO: if we find the exact same <key, value> pair when
                    // duplicates are enabled,
                    // does the insert fail? Or does it "succeed" but we don't
                    // have to do anything?
                    inserted = true;
                    install_new = false;
                  } else {
                    // We can insert this <key, value> pair because duplicates
                    // are enabled and this
                    // value does not exist in locations_
                    inserted = true;
                    install_new = true;
                    // Copy the existing data items into our new items list
                    data_items = key_values.second;
                  }
                }
                break;
              }
            }
            if (!found_key) {
              // Insert this new key-value pair into the tree
              inserted = true;
              install_new = true;
            }
            if (inserted && install_new) {
              // Install new ModifyDelta page
              // Copy old locations_ and add our new value
              data_items.push_back(data);
              int size_data = data_items.size();
              LOG_DEBUG("Creating new modify delta size: %d", size_data);
              ModifyDelta* new_modify_delta = new ModifyDelta(key, data_items);
              new_modify_delta->SetDeltaNext(head_of_delta);

              // If prepending the IndexTermDelta fails, we need to free the
              // resource
              // and start over the insert again.
              if (!map_table_[current_PID].compare_exchange_strong(
                      head_of_delta, new_modify_delta)) {
                // TODO: Garbage collect new_modify_delta.
                LOG_DEBUG("CAS failed");
                attempt_insert = false;  // Start over
                continue;
              }
            }
            return inserted;
          }
          case MODIFY_DELTA: {
            ModifyDelta* mod_delta =
                reinterpret_cast<ModifyDelta*>(current_page);
            bool inserted;     // Whether this <key, value> pair is inserted
            bool install_new;  // Whether we need to install a new MODIFY_DELTA
            if (equals_(key, mod_delta->key_)) {
              if (mod_delta->locations_.size() == 0) {
                // If the locations_ list is empty then we can insert regardless
                // of the duplicate
                // keys policy (this was a delete operation)
                inserted = true;
                install_new = true;
              } else if (!allow_duplicate_) {
                // Key already exists in tree and duplicates are not allowed
                inserted = false;
                install_new = false;
              } else {
                bool found = false;
                for (const auto& value : mod_delta->locations_) {
                  if (value_equals_(value, data)) {
                    found = true;
                    break;
                  }
                }
                if (found) {
                  // TODO: if we find the exact same <key, value> pair when
                  // duplicates are enabled,
                  // does the insert fail? Or does it "succeed" but we don't
                  // have to do anything?
                  inserted = true;
                  install_new = false;
                } else {
                  // We can insert this <key, value> pair because duplicates are
                  // enabled and this
                  // value does not exist in locations_
                  inserted = true;
                  install_new = true;
                }
              }
            } else {
              // This is not our key so we keep traversing the delta chain
              current_page = current_page->GetDeltaNext();
              continue;
            }
            if (inserted && install_new) {
              // Install new ModifyDelta page
              // Copy old locations_ and add our new value
              std::vector<ValueType> locations(mod_delta->locations_);
              locations.push_back(data);
              ModifyDelta* new_modify_delta = new ModifyDelta(key, locations);
              new_modify_delta->SetDeltaNext(head_of_delta);

              // If prepending the IndexTermDelta fails, we need to free the
              // resource
              // and start over the insert again.
              if (!map_table_[current_PID].compare_exchange_strong(
                      head_of_delta, new_modify_delta)) {
                // TODO: Garbage collect new_modify_delta.
                LOG_DEBUG("CAS failed");
                attempt_insert = false;  // Start over
                continue;
              }
            }
            return inserted;
          }
          default:
            throw IndexException("Unrecognized page type\n");
            break;
        }
      }
    }
  }

  return false;
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker, class ValueEqualityChecker>
bool BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker,
            ValueEqualityChecker>::Delete(const KeyType& key,
                                          const ValueType& data) {
  LOG_DEBUG("Trying delete");
  while (true) {
    Page* root_page = map_table_[root_];

    /* If empty root w/ no children then delete fails */
    if (root_page->GetType() == INNER_NODE &&
        reinterpret_cast<InnerNode*>(root_page)->children_.size() == 0) {
      LOG_DEBUG("Delete false 1");
      return false;
    }

    /* Used to remember path */
    std::stack<PID> pages_visited;
    Page* current_page = root_page;
    PID current_PID = root_;
    Page* head_of_delta = current_page;

    bool attempt_delete = true;
    while (attempt_delete) {
      switch (current_page->GetType()) {
        case INNER_NODE: {
          InnerNode* inner_node = reinterpret_cast<InnerNode*>(current_page);
          assert(inner_node->absolute_min_ ||
                 comparator_(key, inner_node->low_key_) > 0);
          assert(inner_node->absolute_max_ ||
                 comparator_(key, inner_node->high_key_) <= 0);

          // We shouldn't be using this yet since we have no consolidation
          for (const auto& child : inner_node->children_) {
            if (comparator_(key, child.first) <= 0) {
              // Found the correct child
              current_PID = child.second;
              current_page = map_table_[current_PID];
              head_of_delta = current_page;
              break;
            }
          }
          continue;
        }
        case INDEX_TERM_DELTA: {
          IndexTermDelta* idx_delta =
              reinterpret_cast<IndexTermDelta*>(current_page);
          // If this key is > low_separator_ and <= high_separator_ then this is
          // the child we
          // must visit next
          if ((idx_delta->absolute_min_ ||
               comparator_(key, idx_delta->low_separator_) > 0) &&
              (idx_delta->absolute_max_ ||
               comparator_(key, idx_delta->high_separator_) <= 0)) {
            // Follow the side link to this child
            current_PID = idx_delta->side_link_;
            current_page = map_table_[current_PID];
            head_of_delta = current_page;
            LOG_DEBUG("Found INDEX_TERM_DELTA to delete from");
          } else {
            // This key does not fall within the boundary represented by this
            // index term
            // delta record. Keep traversing the delta chain.
            current_page = current_page->GetDeltaNext();
          }
          continue;
        }
        case SPLIT_DELTA: {
          break;
        }
        case REMOVE_NODE_DELTA: {
          break;
        }
        case NODE_MERGE_DELTA: {
          break;
        }
        case LEAF_NODE: {
          __attribute__((unused)) LeafNode* leaf =
              reinterpret_cast<LeafNode*>(current_page);

          bool found_location = false;
          std::vector<ValueType> data_items;
          for (const auto& key_values : leaf->data_items_) {
            if (equals_(key, key_values.first)) {
              if (!allow_duplicate_) {
                /* Only one possible instance of this key so done here */
                found_location = true;
                break;
              } else {
                /* Iterate over locations */
                for (const auto& value : key_values.second) {
                  if (value_equals_(value, data)) {
                    found_location = true;
                  } else {
                    /* All items w/ != location are kept */
                    data_items.push_back(value);
                  }
                }
              }

              /* No need to search the rest of the values */
              break;
            }
          }

          /* If location not found delete fails */
          if (!found_location) {
            return false;
          }

          int size_data = data_items.size();
          LOG_DEBUG("Creating new modify delta size: %d", size_data);
          ModifyDelta* new_modify_delta = new ModifyDelta(key, data_items);
          new_modify_delta->SetDeltaNext(head_of_delta);

          if (!map_table_[current_PID].compare_exchange_strong(
                  head_of_delta, new_modify_delta)) {
            // TODO: Garbage collect new_modify_delta.
            LOG_DEBUG("CAS failed");
            attempt_delete = false;  // Start over
            continue;
          }

          return found_location;
        }
        case MODIFY_DELTA: {
          ModifyDelta* mod_delta = reinterpret_cast<ModifyDelta*>(current_page);

          bool found_location = false;
          if (equals_(key, mod_delta->key_)) {
            std::vector<ValueType> data_items;

            /* If empty means nothing to delete */
            if (mod_delta->locations_.size() == 0) {
              return false;
            }

            if (!allow_duplicate_) {
              data_items.resize(0);
              found_location = true;
            } else {
              for (const auto& value : mod_delta->locations_) {
                if (value_equals_(value, data)) {
                  found_location = true;
                } else {
                  /* All items w/ != location are kept */
                  data_items.push_back(value);
                }
              }
            }

            /* Didn't find location to delete */
            if (!found_location) {
              return false;
            }

            int size_data = data_items.size();
            LOG_DEBUG("Creating new modify delta size: %d", size_data);
            ModifyDelta* new_modify_delta = new ModifyDelta(key, data_items);
            new_modify_delta->SetDeltaNext(head_of_delta);

            if (!map_table_[current_PID].compare_exchange_strong(
                    head_of_delta, new_modify_delta)) {
              // TODO: Garbage collect new_modify_delta.
              LOG_DEBUG("CAS failed");
              attempt_delete = false;  // Start over
              continue;
            }
          } else {
            current_page = current_page->GetDeltaNext();
            continue;
          }

          return found_location;
        }
        default:
          throw IndexException("Unrecognized page type\n");
          break;
      }
    }
  }

  /* Deletion Failed */
  return false;
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker, class ValueEqualityChecker>
std::vector<ValueType>
BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker,
       ValueEqualityChecker>::SearchKey(__attribute__((unused))
                                        const KeyType& key) {
  Page* current_page = map_table_[root_];
  PID current_PID = root_;
  __attribute__((unused)) Page* head_of_delta = current_page;
  while (true) {
    switch (current_page->GetType()) {
      case INNER_NODE: {
        InnerNode* inner_node = reinterpret_cast<InnerNode*>(current_page);
        assert(inner_node->absolute_min_ ||
               comparator_(key, inner_node->low_key_) > 0);
        assert(inner_node->absolute_max_ ||
               comparator_(key, inner_node->high_key_) <= 0);
        // TODO: use binary search here instead
        bool found_child = false;  // For debug only, should remove later
        for (const auto& child : inner_node->children_) {
          if (comparator_(key, child.first) <= 0) {
            // We need to go to this child next
            current_PID = child.second;
            current_page = map_table_[current_PID];
            head_of_delta = current_page;
            found_child = true;
            break;
          }
        }
        // We should always find a child to visit next
        if (!found_child)
          // compiler complaining
          assert(found_child);
        continue;
      }
      case INDEX_TERM_DELTA: {
        IndexTermDelta* idx_delta =
            reinterpret_cast<IndexTermDelta*>(current_page);
        // If this key is > low_separator_ and <= high_separator_ then this is
        // the child we
        // must visit next
        if ((idx_delta->absolute_min_ ||
             comparator_(key, idx_delta->low_separator_) > 0) &&
            (idx_delta->absolute_max_ ||
             comparator_(key, idx_delta->high_separator_) <= 0)) {
          // Follow the side link to this child
          current_PID = idx_delta->side_link_;
          current_page = map_table_[current_PID];
          head_of_delta = current_page;
          LOG_DEBUG("Found INDEX_TERM_DELTA to search into");
        } else {
          // This key does not fall within the boundary represented by this
          // index term
          // delta record. Keep traversing the delta chain.
          current_page = current_page->GetDeltaNext();
        }
        continue;
      }
      case SPLIT_DELTA: {
        break;
      }
      case REMOVE_NODE_DELTA: {
        break;
      }
      case NODE_MERGE_DELTA: {
        break;
      }
      case LEAF_NODE: {
        LeafNode* leaf = reinterpret_cast<LeafNode*>(current_page);

        // TODO: this lookup should be binary search
        // Check if the given key is already located in this leaf
        std::vector<ValueType> data_items;
        for (const auto& key_values : leaf->data_items_) {
          if (equals_(key, key_values.first)) {
            return key_values.second;
          }
        }
        return std::vector<ValueType>();
      }
      case MODIFY_DELTA: {
        ModifyDelta* mod_delta = reinterpret_cast<ModifyDelta*>(current_page);
        if (equals_(key, mod_delta->key_)) {
          return mod_delta->locations_;
        } else {
          // This is not our key so we keep traversing the delta chain
          current_page = current_page->GetDeltaNext();
          continue;
        }
      }
      default:
        throw IndexException("Unrecognized page type\n");
        break;
    }

    //return std::vector<ValueType>();
  }
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
