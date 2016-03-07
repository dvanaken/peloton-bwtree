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

namespace peloton {
namespace index {

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker, class ValueEqualityChecker>
BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker,
       ValueEqualityChecker>::BWTree(const KeyComparator& comparator,
                                     const KeyEqualityChecker& equals)
    : comparator_(comparator),
      equals_(equals),
      reverse_comparator_(comparator) {
  root_ = 0;
  PID_counter_ = 1;
  allow_duplicate_ = true;

  InnerNode* root_base_page = new InnerNode();
  map_table_[root_] = root_base_page;

  // Can't do this here because we're using atomic inside vector
  // map_table_.resize(1000000);
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker, class ValueEqualityChecker>
BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker,
       ValueEqualityChecker>::~BWTree() {
  for (PID i = 0; i < PID_counter_; ++i) {
    Page* page = map_table_[i];
    Page* free_page;
    while (page != nullptr) {
      LOG_DEBUG("Freeing page in delta chain for PID %d\n", (int)i);
      free_page = page;
      page = page->GetDeltaNext();
      switch (free_page->GetType()) {
        case INNER_NODE: {
          InnerNode* inner_node = reinterpret_cast<InnerNode*>(free_page);
          delete inner_node;
          break;
        }
        case INDEX_TERM_DELTA: {
          IndexTermDelta* idx_delta =
              reinterpret_cast<IndexTermDelta*>(free_page);
          delete idx_delta;
          break;
        }
        case LEAF_NODE: {
          LeafNode* leaf = reinterpret_cast<LeafNode*>(free_page);
          delete leaf;
          break;
        }
        case MODIFY_DELTA: {
          ModifyDelta* mod_delta =
              reinterpret_cast<ModifyDelta*>(free_page);
          delete mod_delta;
          break;
        }
        case SPLIT_DELTA: {
          SplitDelta* split_delta =
              reinterpret_cast<SplitDelta*>(free_page);
          delete split_delta;
          break;
        }
        case REMOVE_NODE_DELTA: {
          RemoveNodeDelta* remove_delta =
              reinterpret_cast<RemoveNodeDelta*>(free_page);
          delete remove_delta;
          break;
        }
        case NODE_MERGE_DELTA: {
          NodeMergeDelta* merge_delta =
              reinterpret_cast<NodeMergeDelta*>(free_page);
          delete merge_delta;
          break;
        }
        default:
          throw IndexException("Unrecognized page type\n");
          break;
      }
    }
  }
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
        delete index_term_page;

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
                   reverse_comparator_(key, inner_node->low_key_) > 0);
            assert(inner_node->absolute_max_ ||
                   reverse_comparator_(key, inner_node->high_key_) <= 0);

            // TODO: use binary search here instead
            bool found_child = false;  // For debug only, should remove later
            for (const auto& child : inner_node->children_) {
              if (reverse_comparator_(key, child.first) <= 0) {
                pages_visited.push(current_PID);
                // We need to go to this child next
                current_PID = child.second;
                current_page = map_table_[current_PID];
                head_of_delta = current_page;
                found_child = true;
                break;
              }
            }
            // We should always find a child to visit next
            if (!found_child) {
              // If the key is between high_key_ and max value, then we go to
              // the last child.
              if (inner_node->absolute_max_) {
                pages_visited.push(current_PID);
                current_PID = inner_node->children_.rbegin()->second;
                current_page = map_table_[current_PID];
                head_of_delta = current_page;
              } else {
                /* Means we need to repair a split  */
                LOG_DEBUG("Need to complete split SMO");

                bool split_completed =
                    complete_the_split(inner_node->side_link_, pages_visited);

                /* If it fails restart the insert */
                if (!split_completed) {
                  attempt_insert = false;
                  LOG_DEBUG("Split SMO completion failed");
                  continue;
                } else {
                  LOG_DEBUG("Split SMO completion succeeded");
                }

                /* Don't need to add current page to stack b/c it's not the
                 * parent */
                current_PID = inner_node->side_link_;
                current_page = map_table_[current_PID];
                head_of_delta = current_page;
              }
            }
            continue;
          }
          case INDEX_TERM_DELTA: {
            IndexTermDelta* idx_delta =
                reinterpret_cast<IndexTermDelta*>(current_page);
            // If this key is > low_separator_ and <= high_separator_ then this
            // is the child we
            // must visit next
            if ((idx_delta->absolute_min_ ||
                 reverse_comparator_(key, idx_delta->low_separator_) > 0) &&
                (idx_delta->absolute_max_ ||
                 reverse_comparator_(key, idx_delta->high_separator_) <= 0)) {
              // Follow the side link to this child
              pages_visited.push(current_PID);
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
            SplitDelta* split_delta =
                reinterpret_cast<SplitDelta*>(current_page);

            /* Check if your Key is affected by the split */
            if (reverse_comparator_(key, split_delta->separator_) > 0) {
              LOG_DEBUG("Need to complete split SMO");

              /* Need to fix this before continuing */
              bool split_completed =
                  complete_the_split(split_delta->side_link_, pages_visited);

              /* If it fails restart the insert */
              if (!split_completed) {
                attempt_insert = false;
                LOG_DEBUG("Split SMO completion failed");
                continue;
              } else {
                LOG_DEBUG("Split SMO completion succeeded");
              }

              // Don't need to add current page to stack b/c it's not the parent
              current_PID = split_delta->side_link_;
              current_page = map_table_[current_PID];
              head_of_delta = current_page;
            } else {
              current_page = current_page->GetDeltaNext();
            }

            continue;
          }
          case REMOVE_NODE_DELTA: {
            RemoveNodeDelta* remove_delta =
                reinterpret_cast<RemoveNodeDelta*>(current_page);

            /* You need to attempt to complete the merge */
            if (!complete_the_merge(remove_delta, pages_visited)) {
              LOG_DEBUG("Unable to complete the merge");
              attempt_insert = false;
              continue;
            } else {
              LOG_DEBUG("Merge SMO completion succeeded");
            }

            // Don't need to add current page to stack b/c it's not the parent
            current_PID = remove_delta->merged_into_;
            current_page = map_table_[current_PID];
            head_of_delta = current_page;
            continue;
          }
          case NODE_MERGE_DELTA: {
            NodeMergeDelta* merge_delta =
                reinterpret_cast<NodeMergeDelta*>(current_page);

            /* Check if your Key is in merged node  */
            if (reverse_comparator_(key, merge_delta->separator_) >= 0) {
              current_page = merge_delta->physical_link_;
            } else {
              current_page = current_page->GetDeltaNext();
            }

            continue;
          }
          case LEAF_NODE: {
            __attribute__((unused)) LeafNode* leaf =
                reinterpret_cast<LeafNode*>(current_page);
            bool inserted;  // Whether this <key, value> pair is inserted

            /* Check if we need to complete split SMO */
            if (!leaf->absolute_max_ && (reverse_comparator_(key, leaf->high_key_) > 0)) {
              /* Need to fix this before continuing */
              bool split_completed =
                  complete_the_split(leaf->side_link_, pages_visited);

              /* If it fails restart the insert */
              if (!split_completed) {
                attempt_insert = false;
                LOG_DEBUG("Split SMO completion failed");
                continue;
              } else {
                LOG_DEBUG("Split SMO completion succeeded");
              }

              // Don't need to add current page to stack b/c it's not the parent
              current_PID = leaf->side_link_;
              current_page = map_table_[current_PID];
              head_of_delta = current_page;
            }

            // TODO: this lookup should be binary search
            // Check if the given key is already located in this leaf
            bool found_key = false;
            std::vector<ValueType> data_items;
            for (const auto& key_values : leaf->data_items_) {
              if (equals_(key, key_values.first)) {
                found_key = true;
                // Make copy of existing values if duplicates are allowed
                if (allow_duplicate_) data_items = key_values.second;
                break;
              }
            }
            if (!found_key || (found_key && allow_duplicate_)) {
              // Insert this new key-value pair into the tree
              // We can insert this key-value pair because either:
              // 1. It does not yet exist in the tree
              // 2. It does but duplicates are allowed
              inserted = true;
            } else {
              // We cannot insert this key because it's in the tree and
              // duplicates
              // are not allowed
              inserted = false;
            }
            if (inserted) {
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
                // Free the page not contained in mapping_table
                delete new_modify_delta;
                LOG_DEBUG("CAS failed");
                attempt_insert = false;  // Start over
                continue;
              }

              assert(attempt_insert);

              /* Check if consolidation required and perform it if it's */
              if (CheckConsolidate(current_PID)) {
                LOG_DEBUG("Performing consolidation");
                Page* consolidated_page = Consolidate(current_PID);
                if (consolidated_page) {
                  Page* new_modify_delta_page =
                      reinterpret_cast<Page*>(new_modify_delta);

                  /* Attempt to insert updated consolidated page */
                  if (!map_table_[current_PID].compare_exchange_strong(
                          new_modify_delta_page, consolidated_page)) {
                    delete consolidated_page;
                    LOG_DEBUG("CAS of consolidation failed");
                  } else {
                    // TODO: Eventually we'll have epoch garbage collection. But
                    // now we free in this way.
                    FreeDeltaChain(new_modify_delta_page);
                    LOG_DEBUG("CAS of consolidation success");

                    /* Check if split is required and perform the operation */
                    Split_Operation(consolidated_page, pages_visited,
                                    current_PID);

                    /* Check if merge is requires and perform the operation */
                    Merge_Operation(consolidated_page, pages_visited,
                                    current_PID);
                  }
                }
              }
            }
            return inserted;
          }
          case MODIFY_DELTA: {
            ModifyDelta* mod_delta =
                reinterpret_cast<ModifyDelta*>(current_page);
            bool inserted;  // Whether this <key, value> pair is inserted
            if (equals_(key, mod_delta->key_)) {
              if (!allow_duplicate_ && mod_delta->locations_.size() > 0) {
                // Key already exists in tree and duplicates are not allowed
                // A locations_ with size > 0 means this is not a delete delta
                inserted = false;
              } else {
                // We can insert the key because either:
                // 1. Duplicates are NOT allowed but this is a delete modify
                // delta
                //    so this key DNE in the tree anymore
                // 2. Duplicates are allowed so anything goes
                inserted = true;
              }
            } else {
              // This is not our key so we keep traversing the delta chain
              current_page = current_page->GetDeltaNext();
              continue;
            }
            if (inserted) {
              // Install new ModifyDelta page
              // Copy old locations_ and add our new value
              std::vector<ValueType> locations(mod_delta->locations_);
              locations.push_back(data);
              ModifyDelta* new_modify_delta = new ModifyDelta(key, locations);
              new_modify_delta->SetDeltaNext(head_of_delta);

              // If prepending the IndexTermDelta fails, we need to free the
              // resource and start over the insert again.
              if (!map_table_[current_PID].compare_exchange_strong(
                      head_of_delta, new_modify_delta)) {
                // Free the page not contained in mapping_table
                delete new_modify_delta;

                LOG_DEBUG("CAS failed");
                attempt_insert = false;  // Start over
                continue;
              }

              assert(attempt_insert);

              /* Check if consolidation required and perform it if it's */
              if (CheckConsolidate(current_PID)) {
                LOG_DEBUG("Performing consolidation");
                Page* consolidated_page = Consolidate(current_PID);
                if (consolidated_page) {
                  Page* new_modify_delta_page =
                      reinterpret_cast<Page*>(new_modify_delta);

                  /* Attempt to insert updated consolidated page */
                  if (!map_table_[current_PID].compare_exchange_strong(
                          new_modify_delta_page, consolidated_page)) {
                    delete consolidated_page;
                    LOG_DEBUG("CAS of consolidation failed");
                  } else {
                    // TODO: Eventually we'll have epoch garbage collection. But
                    // now we free in this way.
                    FreeDeltaChain(new_modify_delta_page);
                    LOG_DEBUG("CAS of consolidation success");

                    /* Check if split is required and perform the operation */
                    Split_Operation(consolidated_page, pages_visited,
                                    current_PID);

                    /* Check if merge is requires and perform the operation */
                    Merge_Operation(consolidated_page, pages_visited,
                                    current_PID);
                  }
                }
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
                 reverse_comparator_(key, inner_node->low_key_) > 0);
          assert(inner_node->absolute_max_ ||
                 reverse_comparator_(key, inner_node->high_key_) <= 0);

          bool found_child = false;
          // We shouldn't be using this yet since we have no consolidation
          for (const auto& child : inner_node->children_) {
            if (reverse_comparator_(key, child.first) <= 0) {
              pages_visited.push(current_PID);
              // Found the correct child
              current_PID = child.second;
              current_page = map_table_[current_PID];
              head_of_delta = current_page;
              found_child = true;
              break;
            }
          }

          // We should always find a child to visit next
          if (!found_child) {
            // If the key is between high_key_ and max value, then we go to
            // the last child.
            if (inner_node->absolute_max_) {
              pages_visited.push(current_PID);
              current_PID = inner_node->children_.rbegin()->second;
              current_page = map_table_[current_PID];
              head_of_delta = current_page;
            } else {
              /* Means we need to repair a split  */
              LOG_DEBUG("Need to complete split SMO");

              bool split_completed =
                  complete_the_split(inner_node->side_link_, pages_visited);

              /* If it fails restart the insert */
              if (!split_completed) {
                attempt_delete = false;
                LOG_DEBUG("Split SMO completion failed");
                continue;
              } else {
                LOG_DEBUG("Split SMO completion succeeded");
              }

              /* Don't need to add current page to stack b/c it's not the parent
               */
              current_PID = inner_node->side_link_;
              current_page = map_table_[current_PID];
              head_of_delta = current_page;
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
               reverse_comparator_(key, idx_delta->low_separator_) > 0) &&
              (idx_delta->absolute_max_ ||
               reverse_comparator_(key, idx_delta->high_separator_) <= 0)) {
            // Follow the side link to this child
            pages_visited.push(current_PID);
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
          SplitDelta* split_delta = reinterpret_cast<SplitDelta*>(current_page);

          /* Check if your Key is affected by the split */
          if (reverse_comparator_(key, split_delta->separator_) > 0) {
            LOG_DEBUG("Need to complete split SMO");

            /* Need to fix this before continuing */
            bool split_completed =
                complete_the_split(split_delta->side_link_, pages_visited);

            /* If it fails restart the insert */
            if (!split_completed) {
              attempt_delete = false;
              LOG_DEBUG("Split SMO completion failed");
              continue;
            } else {
              LOG_DEBUG("Split SMO completion succeeded");
            }

            // Don't need to add current page to stack b/c it's not the parent
            current_PID = split_delta->side_link_;
            current_page = map_table_[current_PID];
            head_of_delta = current_page;
          } else {
            current_page = current_page->GetDeltaNext();
          }

          continue;
        }
        case REMOVE_NODE_DELTA: {
          RemoveNodeDelta* remove_delta =
              reinterpret_cast<RemoveNodeDelta*>(current_page);

          /* You need to attempt to complete the merge */
          if (!complete_the_merge(remove_delta, pages_visited)) {
            LOG_DEBUG("Unable to complete the merge");
            attempt_delete = false;
            continue;
          } else {
            LOG_DEBUG("Merge SMO completion succeeded");
          }

          // Don't need to add current page to stack b/c it's not the parent
          current_PID = remove_delta->merged_into_;
          current_page = map_table_[current_PID];
          head_of_delta = current_page;
          continue;
        }
        case NODE_MERGE_DELTA: {
          NodeMergeDelta* merge_delta =
              reinterpret_cast<NodeMergeDelta*>(current_page);

          /* Check if your Key is in merged node  */
          if (reverse_comparator_(key, merge_delta->separator_) >= 0) {
            current_page = merge_delta->physical_link_;
          } else {
            current_page = current_page->GetDeltaNext();
          }

          continue;
        }
        case LEAF_NODE: {
          __attribute__((unused)) LeafNode* leaf =
              reinterpret_cast<LeafNode*>(current_page);

          /* Check if we need to complete split SMO */
          if (!leaf->absolute_max_ && (reverse_comparator_(key, leaf->high_key_) > 0)) {
            /* Need to fix this before continuing */
            bool split_completed =
                complete_the_split(leaf->side_link_, pages_visited);

            /* If it fails restart the insert */
            if (!split_completed) {
              attempt_delete = false;
              LOG_DEBUG("Split SMO completion failed");
              continue;
            } else {
              LOG_DEBUG("Split SMO completion succeeded");
            }

            // Don't need to add current page to stack b/c it's not the parent
            current_PID = leaf->side_link_;
            current_page = map_table_[current_PID];
            head_of_delta = current_page;
          }

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
            // Free the page not contained in mapping_table
            delete new_modify_delta;
            LOG_DEBUG("CAS failed");
            attempt_delete = false;  // Start over
            continue;
          }

          assert(attempt_delete);

          /* Check if consolidation required and perform it if it's */
          if (CheckConsolidate(current_PID)) {
            LOG_DEBUG("Performing consolidation");
            Page* consolidated_page = Consolidate(current_PID);
            if (consolidated_page) {
              Page* new_modify_delta_page =
                  reinterpret_cast<Page*>(new_modify_delta);

              /* Attempt to insert updated consolidated page */
              if (!map_table_[current_PID].compare_exchange_strong(
                      new_modify_delta_page, consolidated_page)) {
                delete consolidated_page;
                LOG_DEBUG("CAS of consolidation failed");
              } else {
                // TODO: Eventually we'll have epoch garbage collection. But now
                // we free in this way.
                FreeDeltaChain(new_modify_delta_page);
                LOG_DEBUG("CAS of consolidation success");

                /* Check if split is required and perform the operation */
                Split_Operation(consolidated_page, pages_visited, current_PID);

                /* Check if merge is requires and perform the operation */
                Merge_Operation(consolidated_page, pages_visited, current_PID);
              }
            }
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
              // Free the page not contained in mapping_table
              delete new_modify_delta;
              LOG_DEBUG("CAS failed");
              attempt_delete = false;  // Start over
              continue;
            }

            assert(attempt_delete);

            /* Check if consolidation required and perform it if it's */
            if (CheckConsolidate(current_PID)) {
              LOG_DEBUG("Performing consolidation");
              Page* consolidated_page = Consolidate(current_PID);
              if (consolidated_page) {
                Page* new_modify_delta_page =
                    reinterpret_cast<Page*>(new_modify_delta);

                /* Attempt to insert updated consolidated page */
                if (!map_table_[current_PID].compare_exchange_strong(
                        new_modify_delta_page, consolidated_page)) {
                  delete consolidated_page;
                  LOG_DEBUG("CAS of consolidation failed");
                } else {
                  // TODO: Eventually we'll have epoch garbage collection. But
                  // now we free in this way.
                  FreeDeltaChain(new_modify_delta_page);

                  LOG_DEBUG("CAS of consolidation success");

                  /* Check if split is required and perform the operation */
                  Split_Operation(consolidated_page, pages_visited,
                                  current_PID);

                  /* Check if merge is requires and perform the operation */
                  Merge_Operation(consolidated_page, pages_visited,
                                  current_PID);
                }
              }
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

  /* If empty root w/ no children then search fails */
  if (current_page->GetType() == INNER_NODE &&
      reinterpret_cast<InnerNode*>(current_page)->children_.size() == 0) {
    LOG_DEBUG("SearchKey returning nothing because BWTree is empty");
    return std::vector<ValueType>();
  }
  __attribute__((unused)) Page* head_of_delta = current_page;
  while (true) {
    switch (current_page->GetType()) {
      case INNER_NODE: {
        InnerNode* inner_node = reinterpret_cast<InnerNode*>(current_page);
        if (reverse_comparator_(key, inner_node->high_key_) > 0 &&
            inner_node->side_link_ != NullPID) {
          current_PID = inner_node->side_link_;
          current_page = map_table_[current_PID];
          head_of_delta = current_page;
          break;
        }
        assert(inner_node->absolute_min_ ||
               reverse_comparator_(key, inner_node->low_key_) > 0);
        assert(inner_node->absolute_max_ ||
               reverse_comparator_(key, inner_node->high_key_) <= 0);
        // TODO: use binary search here instead
        bool found_child = false;  // For debug only, should remove later
        for (const auto& child : inner_node->children_) {
          if (reverse_comparator_(key, child.first) <= 0) {
            // We need to go to this child next
            current_PID = child.second;
            current_page = map_table_[current_PID];
            head_of_delta = current_page;
            found_child = true;
            break;
          }
        }

        // We should always find a child to visit next
        if (!found_child) {
          // If the key is between high_key_ and max value, then we go to
          // the last child.
          if (inner_node->absolute_max_) {
            current_PID = inner_node->children_.rbegin()->second;
          } else {
            current_PID = inner_node->side_link_;
          }
          current_page = map_table_[current_PID];
          head_of_delta = current_page;
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
             reverse_comparator_(key, idx_delta->low_separator_) > 0) &&
            (idx_delta->absolute_max_ ||
             reverse_comparator_(key, idx_delta->high_separator_) <= 0)) {
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
        SplitDelta* split_delta = reinterpret_cast<SplitDelta*>(current_page);

        /* Check if your Key is affected by the split */
        if (reverse_comparator_(key, split_delta->separator_) > 0) {
          current_PID = split_delta->side_link_;
          current_page = map_table_[current_PID];
          head_of_delta = current_page;
        } else {
          current_page = current_page->GetDeltaNext();
        }
        break;
      }
      case REMOVE_NODE_DELTA: {
        current_page = current_page->GetDeltaNext();
        break;
      }
      case NODE_MERGE_DELTA: {
        NodeMergeDelta* merge_delta =
            reinterpret_cast<NodeMergeDelta*>(current_page);

        /* Check if the key is in the merged node */
        if (reverse_comparator_(key, merge_delta->separator_) > 0) {
          current_page = merge_delta->physical_link_;
        } else {
          current_page = current_page->GetDeltaNext();
        }
        break;
      }
      case LEAF_NODE: {
        LeafNode* leaf = reinterpret_cast<LeafNode*>(current_page);

        if (reverse_comparator_(key, leaf->high_key_) > 0 &&
            leaf->side_link_ != NullPID) {
          current_PID = leaf->side_link_;
          current_page = map_table_[current_PID];
          head_of_delta = current_page;
          break;
        }

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

    // return std::vector<ValueType>();
  }
}

/*
template <typename KeyType, typename KeyEqualityChecker>
class VisitedChecker {
 public:
  std::vector<KeyType> data_;

  void insert(const KeyType& item) { data_.push_back(item); }

  bool find(const KeyType& item, KeyEqualityChecker& equals) {
    for (auto& i : data_) {
      if (equals(i, item)) return true;
    }
    return false;
  }
};
*/

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker, class ValueEqualityChecker>
std::vector<ValueType>
BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker,
       ValueEqualityChecker>::SearchAllKeys() {
  LOG_DEBUG("Trying searchAllKeys");
  std::vector<ValueType> result;

  Page* current_page = map_table_[root_];
  PID current_PID = root_;
  /* If empty root w/ no children then search fails */
  if (current_page->GetType() == INNER_NODE &&
      reinterpret_cast<InnerNode*>(current_page)->children_.size() == 0) {
    LOG_DEBUG("SearchAllKeys returning nothing because BWTree is empty");
    return result;
  }

  // Finally I found out a way to use set here... so we dont't need this
  // VisitedChecker<KeyType, KeyEqualityChecker> visited_keys;

  std::set<KeyType, KeyComparator> visited_keys(comparator_);
  bool split_indicator = false;
  bool merge_indicator = false;
  KeyType split_separator;
  PID split_next_node;

  __attribute__((unused)) Page* head_of_delta = current_page;
  while (true) {
    switch (current_page->GetType()) {
      case INNER_NODE: {
        InnerNode* inner_node = reinterpret_cast<InnerNode*>(current_page);
        if (inner_node->children_.size() == 0) {
          return result;
        } else {
          current_PID = inner_node->children_[0].second;
          current_page = map_table_[current_PID];
          head_of_delta = current_page;
        }
        continue;
      }
      case INDEX_TERM_DELTA: {
        IndexTermDelta* idx_delta =
            reinterpret_cast<IndexTermDelta*>(current_page);

        // We always want to go to the left most first
        if (idx_delta->absolute_min_) {
          // Follow the side link to this child
          current_PID = idx_delta->side_link_;
          current_page = map_table_[current_PID];
          head_of_delta = current_page;
          LOG_DEBUG("Found INDEX_TERM_DELTA to search the left most key into");
        } else {
          // Keep traversing the delta chain.
          current_page = current_page->GetDeltaNext();
        }
        continue;
      }
      case SPLIT_DELTA: {
        SplitDelta* split_delta = reinterpret_cast<SplitDelta*>(current_page);
        split_indicator = true;
        split_separator = split_delta->separator_;
        split_next_node = split_delta->side_link_;
        current_page = split_delta->GetDeltaNext();
        break;
      }
      case REMOVE_NODE_DELTA: {
        current_page = current_page->GetDeltaNext();
        break;
      }
      case NODE_MERGE_DELTA: {
        merge_indicator = true;
        current_page = current_page->GetDeltaNext();
        break;
      }
      case LEAF_NODE: {
        LOG_DEBUG("SearchAllKeys into one leaf node");
        LeafNode* leaf = reinterpret_cast<LeafNode*>(current_page);

        // Traverse all data item in the leaf. If we already visited before,
        // then skip.
        std::vector<ValueType> data_items;
        for (const auto& key_values : leaf->data_items_) {
          if (split_indicator &&
              reverse_comparator_(key_values.first, split_separator) > 0)
            break;
          if (visited_keys.find(key_values.first) == visited_keys.end()) {
            // if (visited_keys.find(key_values.first) == visited_keys.end()) {
            visited_keys.insert(key_values.first);
            result.insert(result.end(), key_values.second.begin(),
                          key_values.second.end());
          }
        }

        if (!merge_indicator) visited_keys.clear();

        if (split_indicator) {
          current_PID = split_next_node;
        } else {
          if (leaf->next_leaf_ != NullPID)
            current_PID = leaf->next_leaf_;
          else
            return result;
        }

        current_page = map_table_[current_PID];
        head_of_delta = current_page;

        split_indicator = false;
        merge_indicator = false;

        break;
      }
      case MODIFY_DELTA: {
        ModifyDelta* mod_delta = reinterpret_cast<ModifyDelta*>(current_page);
        if (visited_keys.find(mod_delta->key_) == visited_keys.end()) {
          // if (visited_keys.find(mod_delta->key_) == visited_keys.end()) {
          visited_keys.insert(mod_delta->key_);
          result.insert(result.end(), mod_delta->locations_.begin(),
                        mod_delta->locations_.end());
        }

        current_page = current_page->GetDeltaNext();
        continue;
      }
      default:
        throw IndexException("Unrecognized page type\n");
        break;
    }

    // return std::vector<ValueType>();
  }
}

// TODO: Need to handle case where we are splitting the root
template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker, class ValueEqualityChecker>
void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker,
            ValueEqualityChecker>::Split_Operation(Page* consolidated_page,
                                                   std::stack<PID>&
                                                       pages_visited,
                                                   PID orig_pid) {
  bool split_required = false;
  SplitDelta* split_delta;
  IndexTermDelta* index_term_delta_for_split;

  /* Check if split required */
  if (consolidated_page->GetType() == INNER_NODE) {
    InnerNode* node_to_split = reinterpret_cast<InnerNode*>(consolidated_page);
    if (node_to_split->children_.size() > SPLIT_SIZE) {
      split_required = true;

      /* Create new node */
      InnerNode* new_inner_node = new InnerNode();
      new_inner_node->absolute_min_ = false;
      new_inner_node->absolute_max_ = node_to_split->absolute_max_;
      new_inner_node->side_link_ = node_to_split->side_link_;

      /* Choose a key to split on */
      int key_split_index = (node_to_split->children_.size() / 2) - 1;
      KeyType new_separator_key =
          node_to_split->children_[key_split_index].first;
      new_inner_node->low_key_ = new_separator_key;
      new_inner_node->high_key_ = node_to_split->high_key_;

      /* Populate new node */
      for (int i = (key_split_index + 1); i < node_to_split->children_.size();
           i++) {
        // can optimize this in the future
        new_inner_node->children_.push_back(node_to_split->children_[i]);
        assert(reverse_comparator_(node_to_split->children_[i].first,
                                   new_separator_key) > 0);
      }

      /* Install the new page in the mapping table */
      PID new_node_PID = InstallNewMapping(new_inner_node);

      /* Create the Delta Split to Install */
      split_delta = new SplitDelta(new_separator_key, new_node_PID);

      /* Create the index term delta for the parent */
      index_term_delta_for_split = new IndexTermDelta(
          new_inner_node->low_key_, new_inner_node->high_key_, new_node_PID);
      index_term_delta_for_split->absolute_max_ = new_inner_node->absolute_max_;
    }
  } else if (consolidated_page->GetType() == LEAF_NODE) {
    __attribute__((unused)) LeafNode* node_to_split =
        reinterpret_cast<LeafNode*>(consolidated_page);
    if (node_to_split->data_items_.size() > SPLIT_SIZE) {
      split_required = true;

      /* Create new node */
      LeafNode* new_leaf_node = new LeafNode();
      new_leaf_node->absolute_min_ = false;
      new_leaf_node->absolute_max_ = node_to_split->absolute_max_;
      new_leaf_node->side_link_ = node_to_split->side_link_;
      new_leaf_node->prev_leaf_ = node_to_split->prev_leaf_;
      new_leaf_node->next_leaf_ = node_to_split->next_leaf_;

      /* Choose a key to split on */
      int key_split_index = (node_to_split->data_items_.size() / 2) - 1;
      KeyType new_separator_key =
          node_to_split->data_items_[key_split_index].first;
      new_leaf_node->low_key_ = new_separator_key;
      new_leaf_node->high_key_ = node_to_split->high_key_;

      /* Populate new node */
      for (int i = (key_split_index + 1); i < node_to_split->data_items_.size();
           i++) {
        // can optimize this in the future
        new_leaf_node->data_items_.push_back(node_to_split->data_items_[i]);
        assert(reverse_comparator_(node_to_split->data_items_[i].first,
                                   new_separator_key) > 0);
      }

      /* Install the new page in the mapping table */
      PID new_node_PID = InstallNewMapping(new_leaf_node);

      /* Create the Delta Split to Install */
      split_delta = new SplitDelta(new_separator_key, new_node_PID);

      /* Create the index term delta for the parent */
      index_term_delta_for_split = new IndexTermDelta(
          new_leaf_node->low_key_, new_leaf_node->high_key_, new_node_PID);
      index_term_delta_for_split->absolute_max_ = new_leaf_node->absolute_max_;
    }

  } else {
    assert(0);
  }

  /* If split performed try to install split delta */  // delete if fails
  if (split_required) {
    LOG_DEBUG("Performing Split");
    split_delta->SetDeltaNext(consolidated_page);
    if (!map_table_[orig_pid].compare_exchange_strong(consolidated_page,
                                                      split_delta)) {
      delete split_delta;
      delete index_term_delta_for_split;
      LOG_DEBUG("CAS of installing delta split failed");
      return;
    } else {
      LOG_DEBUG("CAS of installing delta split success");

      /* Get PID of parent node */
      PID pid_of_parent = pages_visited.top();

      /* Attempt to install index term delta on the parent */
      while (true) {
        // Safe to keep retrying - thoughts? (aaron)
        Page* parent_node = map_table_[pid_of_parent];
        if (parent_node->GetType() != REMOVE_NODE_DELTA) {
          index_term_delta_for_split->SetDeltaNext(parent_node);
          if (map_table_[pid_of_parent].compare_exchange_strong(
                  parent_node, index_term_delta_for_split)) {
            LOG_DEBUG("CAS of installing index term delta in parent succeeded");
            break;
          }
        } else {
          delete index_term_delta_for_split;
          LOG_DEBUG("CAS of installing index term delta in parent failed");
          return;
        }
      }
    }
  }
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker, class ValueEqualityChecker>
bool BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker,
            ValueEqualityChecker>::complete_the_split(PID side_link,
                                                      std::stack<PID>&
                                                          pages_visited) {
  Page* new_split_node = map_table_[side_link];
  IndexTermDelta* index_term_delta_for_split;

  if (new_split_node->GetType() == INNER_NODE) {
    InnerNode* new_inner_node = reinterpret_cast<InnerNode*>(new_split_node);

    /* Create the index term delta for the parent */
    index_term_delta_for_split = new IndexTermDelta(
        new_inner_node->low_key_, new_inner_node->high_key_, side_link);
    index_term_delta_for_split->absolute_max_ = new_inner_node->absolute_max_;

  } else if (new_split_node->GetType() == LEAF_NODE) {
    __attribute__((unused)) LeafNode* new_leaf_node =
        reinterpret_cast<LeafNode*>(new_split_node);

    /* Create the index term delta for the parent */
    index_term_delta_for_split = new IndexTermDelta(
        new_leaf_node->low_key_, new_leaf_node->high_key_, side_link);
    index_term_delta_for_split->absolute_max_ = new_leaf_node->absolute_max_;
  } else {
    // I believe this should never happen - thoughts? (aaron)
    assert(0);
  }

  /* Get PID of parent node */
  PID pid_of_parent = pages_visited.top();

  /* Attempt to install index term delta on the parent */
  while (true) {
    // Safe to keep retrying - thoughts? (aaron)
    Page* parent_node = map_table_[pid_of_parent];
    if (parent_node->GetType() != REMOVE_NODE_DELTA) {
      index_term_delta_for_split->SetDeltaNext(parent_node);
      if (map_table_[pid_of_parent].compare_exchange_strong(
              parent_node, index_term_delta_for_split)) {
        LOG_DEBUG("CAS of installing index term delta in parent succeeded");
        return true;
      }
    } else {
      delete index_term_delta_for_split;
      LOG_DEBUG("CAS of installing index term delta in parent failed");
      return false;
    }
  }

  return true;
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker, class ValueEqualityChecker>
void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker,
            ValueEqualityChecker>::Merge_Operation(Page* consolidated_page,
                                                   std::stack<PID>&
                                                       pages_visited,
                                                   PID orig_pid) {
  bool merge_required = false;
  RemoveNodeDelta* remove_node_delta;
  NodeMergeDelta* merge_delta;
  IndexTermDelta* index_term_delta_for_merge;
  PID pid_merging_into;

  /* Check if merge required */
  if (consolidated_page->GetType() == INNER_NODE) {
    InnerNode* node_to_merge = reinterpret_cast<InnerNode*>(consolidated_page);
    if ((node_to_merge->children_.size() < MERGE_SIZE) &&
        (!node_to_merge->absolute_min_)) {
      merge_required = true;

      /* Find the node to merge into */
      pid_merging_into =
          find_left_sibling(pages_visited, node_to_merge->low_key_);

      if (pid_merging_into == root_) {
        LOG_DEBUG("Merge abandoned - couldn't find left sibling");
        return;
      }

      /* Consolidate the page we are merging into */
      Page* current_top_of_page = map_table_[pid_merging_into];
      assert(current_top_of_page->GetType() != REMOVE_NODE_DELTA);
      Page* page_merging_into = Consolidate(pid_merging_into);

      if (!page_merging_into) {
        return;
      }

      /* Create new remove node delta */
      remove_node_delta = new RemoveNodeDelta(pid_merging_into);

      /* Create a new merge delta */
      merge_delta =
          new NodeMergeDelta(node_to_merge->high_key_,
                             node_to_merge->absolute_max_, consolidated_page);

      InnerNode* node_merging_into =
          reinterpret_cast<InnerNode*>(page_merging_into);
      assert(node_merging_into->side_link_ == orig_pid);

      /* Create the index term delta for the parent */
      index_term_delta_for_merge =
          new IndexTermDelta(node_merging_into->low_key_,
                             node_to_merge->high_key_, pid_merging_into);
      index_term_delta_for_merge->absolute_max_ = node_to_merge->absolute_max_;
      index_term_delta_for_merge->absolute_min_ =
          node_merging_into->absolute_min_;

      /* Attempt to install the consolidated page */
      if (map_table_[pid_merging_into].compare_exchange_strong(
              current_top_of_page, page_merging_into)) {
        FreeDeltaChain(current_top_of_page);
        LOG_DEBUG(
            "Successfully installed consolidated page we are merging into");
      } else {
        delete page_merging_into;
      }
    }
  } else if (consolidated_page->GetType() == LEAF_NODE) {
    __attribute__((unused)) LeafNode* node_to_merge =
        reinterpret_cast<LeafNode*>(consolidated_page);
    if ((node_to_merge->data_items_.size() < MERGE_SIZE) &&
        (!node_to_merge->absolute_min_)) {
      merge_required = true;

      /* Find the node to merge into */
      pid_merging_into =
          find_left_sibling(pages_visited, node_to_merge->low_key_);

      if (pid_merging_into == root_) {
        LOG_DEBUG("Merge abandoned - couldn't find left sibling");
        return;
      }

      /* Consolidate the page we are merging into */
      Page* current_top_of_page = map_table_[pid_merging_into];
      assert(current_top_of_page->GetType() != REMOVE_NODE_DELTA);
      Page* page_merging_into = Consolidate(pid_merging_into);

      if (!page_merging_into) {
        return;
      }

      /* Create new remove node delta */
      remove_node_delta = new RemoveNodeDelta(pid_merging_into);

      /* Create a new merge delta */
      merge_delta =
          new NodeMergeDelta(node_to_merge->high_key_,
                             node_to_merge->absolute_max_, consolidated_page);

      __attribute__((unused)) LeafNode* node_merging_into =
          reinterpret_cast<LeafNode*>(page_merging_into);
      assert(node_merging_into->side_link_ == orig_pid);

      /* Create the index term delta for the parent */
      index_term_delta_for_merge =
          new IndexTermDelta(node_merging_into->low_key_,
                             node_to_merge->high_key_, pid_merging_into);
      index_term_delta_for_merge->absolute_max_ = node_to_merge->absolute_max_;
      index_term_delta_for_merge->absolute_min_ =
          node_merging_into->absolute_min_;

      /* Attempt to install the consolidated page */
      if (map_table_[pid_merging_into].compare_exchange_strong(
              current_top_of_page, page_merging_into)) {
        // TODO: Eventually we'll have epoch garbage collection. But now we free
        // in this way.
        FreeDeltaChain(current_top_of_page);
        LOG_DEBUG(
            "Successfully installed consolidated page we are merging into");
      } else {
        delete page_merging_into;
      }
    }
  } else {
    assert(0);
  }

  /* Try to install remove, merge, & index_term */
  if (merge_required) {
    LOG_DEBUG("Performing Merge");
    remove_node_delta->SetDeltaNext(consolidated_page);
    if (!map_table_[orig_pid].compare_exchange_strong(consolidated_page,
                                                      remove_node_delta)) {
      LOG_DEBUG("CAS of installing remove node failed");
      delete remove_node_delta;
      delete merge_delta;
      delete index_term_delta_for_merge;
      return;
    } else {
      LOG_DEBUG("CAS of installing remove node success");

      /* Attempt to install node merge delta */
      while (true) {
        Page* page_merging_into = map_table_[pid_merging_into];
        if (page_merging_into->GetType() != REMOVE_NODE_DELTA) {
          merge_delta->SetDeltaNext(page_merging_into);
          if (map_table_[pid_merging_into].compare_exchange_strong(
                  page_merging_into, merge_delta)) {
            LOG_DEBUG("CAS of installing node merge delta succeeded");
            break;
          }
        } else {
          delete merge_delta;
          delete index_term_delta_for_merge;
          LOG_DEBUG("CAS of installing node merge delta failed");
          return;
        }
      }

      /* Get PID of parent node */
      PID pid_of_parent = pages_visited.top();

      /* Attempt to install index term delta on the parent */
      while (true) {
        // Safe to keep retrying - thoughts? (aaron)
        Page* parent_node = map_table_[pid_of_parent];
        if (parent_node->GetType() != REMOVE_NODE_DELTA) {
          index_term_delta_for_merge->SetDeltaNext(parent_node);
          if (map_table_[pid_of_parent].compare_exchange_strong(
                  parent_node, index_term_delta_for_merge)) {
            LOG_DEBUG("CAS of installing index term delta in parent succeeded");
            break;
          }
        } else {
          delete index_term_delta_for_merge;
          LOG_DEBUG("CAS of installing index term delta in parent failed");
          return;
        }
      }
    }
  }
}

// TODO: Handle case where parent is deleted, split, merged
/* Returns root PID if we fail to find child */
template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker, class ValueEqualityChecker>
std::uint64_t
BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker,
       ValueEqualityChecker>::find_left_sibling(std::stack<PID>& pages_visited,
                                                KeyType merge_key) {
  PID parent_pid = pages_visited.top();
  Page* parent_page = map_table_[parent_pid];

  /* Go over the delta chain looking for node neighboring the merge_key */
  while (true) {
    switch (parent_page->GetType()) {
      case INNER_NODE: {
        InnerNode* inner_node = reinterpret_cast<InnerNode*>(parent_page);

        for (const auto& child : inner_node->children_) {
          if (reverse_comparator_(merge_key, child.first) == 0) {
            return child.second;
          }
        }

        /* Couldn't find neighbor */
        return root_;

        break;
      }
      case INDEX_TERM_DELTA: {
        IndexTermDelta* idx_delta =
            reinterpret_cast<IndexTermDelta*>(parent_page);

        if (reverse_comparator_(merge_key, idx_delta->high_separator_) == 0) {
          return idx_delta->side_link_;
        } else {
          parent_page = parent_page->GetDeltaNext();
        }
        continue;
      }
      case SPLIT_DELTA: {
        /* If parent is split abandon merge */
        // if split higher try more
        return root_;
        break;
      }
      case REMOVE_NODE_DELTA: {
        /* If parent is deleted abandon merge */
        return root_;
        break;
      }
      case NODE_MERGE_DELTA: {
        /* If parent is merged abandon merge */
        return root_;
        break;
      }
      case LEAF_NODE: {
        assert(0);
        break;
      }
      case MODIFY_DELTA: {
        parent_page = parent_page->GetDeltaNext();
        continue;
      }
      default:
        throw IndexException("Unrecognized page type\n");
        break;
    }
  }
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker, class ValueEqualityChecker>
bool BWTree<
    KeyType, ValueType, KeyComparator, KeyEqualityChecker,
    ValueEqualityChecker>::complete_the_merge(RemoveNodeDelta* remove_node,
                                              std::stack<PID>& pages_visited) {
  PID megred_into_pid = remove_node->merged_into_;
  IndexTermDelta* index_term_delta;

  /* Get the high key of the page that's deleted */
  Page* page_deleted = remove_node->GetDeltaNext();

  /* Consolidated the page we are merging into */
  Page* page_merging_into = map_table_[megred_into_pid];
  Page* page_merging_into_consolidate = Consolidate(megred_into_pid);

  if (!page_merging_into_consolidate) {
    LOG_DEBUG("Failed to consolidated page merging into");
    return false;
  }

  /* Install consolidated page being merged into */
  if (!map_table_[megred_into_pid].compare_exchange_strong(
          page_merging_into, page_merging_into_consolidate)) {
    delete page_merging_into_consolidate;
    LOG_DEBUG("CAS of consolidated page failed");
    return false;
  } else {
    // TODO: Eventually we'll have epoch garbage collection. But now we free in
    // this way.
    FreeDeltaChain(page_merging_into);
  }

  /* Check if merge delta is present */
  if (page_deleted->GetType() == INNER_NODE) {
    InnerNode* inner_node = reinterpret_cast<InnerNode*>(page_deleted);
    if (!is_merge_installed(page_merging_into_consolidate, page_deleted,
                            inner_node->high_key_)) {
      LOG_DEBUG("Need to install merge delta for incomplete SMO");

      /* Create a new merge delta */
      NodeMergeDelta* merge_delta = new NodeMergeDelta(
          inner_node->high_key_, inner_node->absolute_max_, page_deleted);

      /* Install Merge Delta */
      merge_delta->SetDeltaNext(page_merging_into_consolidate);
      if (!map_table_[megred_into_pid].compare_exchange_strong(
              page_merging_into_consolidate, merge_delta)) {
        LOG_DEBUG("CAS of merge delta failed");
        delete merge_delta;
        return false;
      }

      InnerNode* inner_merged_into =
          reinterpret_cast<InnerNode*>(page_merging_into_consolidate);
      index_term_delta =
          new IndexTermDelta(inner_merged_into->low_key_,
                             inner_merged_into->high_key_, megred_into_pid);
      index_term_delta->absolute_max_ = inner_merged_into->absolute_max_;
      index_term_delta->absolute_min_ = inner_merged_into->absolute_min_;
    }
  } else if (page_deleted->GetType() == LEAF_NODE) {
    __attribute__((unused)) LeafNode* leaf =
        reinterpret_cast<LeafNode*>(page_deleted);
    if (!is_merge_installed(page_merging_into_consolidate, page_deleted,
                            leaf->high_key_)) {
      LOG_DEBUG("Need to install merge delta for incomplete SMO");

      /* Create a new merge delta */
      NodeMergeDelta* merge_delta = new NodeMergeDelta(
          leaf->high_key_, leaf->absolute_max_, page_deleted);

      /* Install Merge Delta */
      merge_delta->SetDeltaNext(page_merging_into_consolidate);
      if (!map_table_[megred_into_pid].compare_exchange_strong(
              page_merging_into_consolidate, merge_delta)) {
        LOG_DEBUG("CAS of merge delta failed");
        delete merge_delta;
        return false;
      }

      __attribute__((unused)) LeafNode* leaf_merged_into =
          reinterpret_cast<LeafNode*>(page_merging_into_consolidate);
      index_term_delta =
          new IndexTermDelta(leaf_merged_into->low_key_,
                             leaf_merged_into->high_key_, megred_into_pid);
      index_term_delta->absolute_max_ = leaf_merged_into->absolute_max_;
      index_term_delta->absolute_min_ = leaf_merged_into->absolute_min_;
    }
  } else {
    assert(0);
  }

  /* Get PID of parent node */
  PID pid_of_parent = pages_visited.top();

  /* Attempt to install index term delta on the parent */
  while (true) {
    Page* parent_node = map_table_[pid_of_parent];
    if (parent_node->GetType() != REMOVE_NODE_DELTA) {
      index_term_delta->SetDeltaNext(parent_node);
      if (map_table_[pid_of_parent].compare_exchange_strong(parent_node,
                                                            index_term_delta)) {
        LOG_DEBUG("CAS of installing index term delta in parent succeeded");
        return true;
      }
    } else {
      delete index_term_delta;
      LOG_DEBUG("CAS of installing index term delta in parent failed because parent is removed");
      return false;
    }
  }
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker, class ValueEqualityChecker>
bool BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker,
            ValueEqualityChecker>::is_merge_installed(Page* page_merging_into,
                                                      Page* compare_to,
                                                      KeyType high_key) {
  Page* current_page = page_merging_into;

  while (true) {
    switch (current_page->GetType()) {
      case INNER_NODE: {
        InnerNode* inner_node = reinterpret_cast<InnerNode*>(current_page);

        if (reverse_comparator_(high_key, inner_node->high_key_) <= 0) {
          return true;
        } else {
          return false;
        }
        continue;
      }
      case INDEX_TERM_DELTA: {
        current_page = current_page->GetDeltaNext();
        continue;
      }
      case SPLIT_DELTA: {
        assert(0);
        continue;
      }
      case REMOVE_NODE_DELTA: {
        assert(0);
        continue;
      }
      case NODE_MERGE_DELTA: {
        NodeMergeDelta* merge_delta =
            reinterpret_cast<NodeMergeDelta*>(current_page);
        if (merge_delta->physical_link_ == compare_to) {
          return true;
        }

        current_page = current_page->GetDeltaNext();
        continue;
      }
      case LEAF_NODE: {
        __attribute__((unused)) LeafNode* leaf =
            reinterpret_cast<LeafNode*>(current_page);

        if (reverse_comparator_(high_key, leaf->high_key_) <= 0) {
          return true;
        } else {
          return false;
        }
        continue;
      }
      case MODIFY_DELTA: {
        current_page = current_page->GetDeltaNext();
        continue;
      }
      default:
        throw IndexException("Unrecognized page type\n");
        break;
    }
  }

  assert(0);
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
