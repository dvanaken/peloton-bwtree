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
#include <chrono>
#include <mutex>
#include <thread>

#include "backend/index/bwtree.h"

namespace peloton {
namespace index {

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker,
          bool _Duplicates ,
          class ValueEqualityChecker>
BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker,
          _Duplicates,
       ValueEqualityChecker>::BWTree(const KeyComparator& comparator,
                                     const KeyEqualityChecker& equals)
    : comparator_(comparator),
      equals_(equals),
      reverse_comparator_(comparator),
      epoch_manager_() {
  root_ = 0;
  PID_counter_ = 1;
  allow_duplicate_ = _Duplicates;

  InnerNode* root_base_page = new InnerNode();
  map_table_[root_] = root_base_page;

  epoch_ = 0;
  epoch_garbage_[epoch_] = std::vector<Page*>();
  if (GC_ENABLED) {
    // Initialize state for epoch manager
    active_threads_map_[epoch_] = 0;
    finished_ = false;
    Start();
  }

  // Can't do this here because we're using atomic inside vector
  // map_table_.resize(1000000);

  // Used for debug: print out the key
  std::vector<catalog::Column> columns;
  catalog::Column column1(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                          "A", true);
  catalog::Column column2(VALUE_TYPE_VARCHAR, 1024, "B", true);
  columns.push_back(column1);
  columns.push_back(column2);
  key_tuple_schema = new catalog::Schema(columns);
  key_tuple_schema->SetIndexedColumns({0, 1});

}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker,
          bool _Duplicates,
          class ValueEqualityChecker>
BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, _Duplicates,
       ValueEqualityChecker>::~BWTree() {
  if (GC_ENABLED) {
    finished_ = true;
    exec_finished_.notify_one();
    epoch_manager_.join();
  }
  delete key_tuple_schema;

  // Free chains in map table
  for (PID i = 0; i < PID_counter_; ++i) {
    Page* page = map_table_[i];
    FreeDeltaChain(page);
  }
  // Free any chains left in the garbage collection queue
  for (auto item : epoch_garbage_) {
    for (Page *page : item.second) {
      FreeDeltaChain(page);
    }
  }
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker,
          bool _Duplicates,
          class ValueEqualityChecker>
bool BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, _Duplicates,
            ValueEqualityChecker>::Insert(const KeyType& key,
                                          const ValueType& data) {
  LOG_DEBUG("Trying new insert");
  __attribute__((unused)) KeyType tmp_key = key;
  LOG_DEBUG("Trying new insert with key: %s", tmp_key.GetTupleForComparison(key_tuple_schema).GetInfo().c_str());
  uint64_t worker_epoch = RegisterWorker();
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
        LOG_DEBUG("CAS of first index_term_page successful");
        DeregisterWorker(worker_epoch);
        return true;
      } else {
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
            LOG_DEBUG("Visit INNER_NODE");
            InnerNode* inner_node = reinterpret_cast<InnerNode*>(current_page);
            assert(inner_node->absolute_min_ ||
                   reverse_comparator_(key, inner_node->low_key_) > 0);
            assert(inner_node->absolute_max_ ||
                   reverse_comparator_(key, inner_node->high_key_) <= 0);

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
            LOG_DEBUG("Visit INDEX_TERM_DELTA");
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
            LOG_DEBUG("Visit SPLIT_DELTA");
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
            LOG_DEBUG("Visit REMOVE_NODE_DELTA");
            RemoveNodeDelta* remove_delta =
                reinterpret_cast<RemoveNodeDelta*>(current_page);

            /* You need to attempt to complete the merge */
            if (!complete_the_merge(remove_delta, pages_visited)) {
              LOG_DEBUG("Unable to complete the merge");
              attempt_insert = false;
              continue;
            } else {
              LOG_DEBUG("Merge SMO completion succeeded with PID %ld into PID %ld", current_PID, remove_delta->merged_into_);
            }

            // Don't need to add current page to stack b/c it's not the parent
            current_PID = remove_delta->merged_into_;
            current_page = map_table_[current_PID];
            head_of_delta = current_page;
            continue;
          }
          case NODE_MERGE_DELTA: {
            LOG_DEBUG("Visit NODE_MERGE_DELTA");
            NodeMergeDelta* merge_delta =
                reinterpret_cast<NodeMergeDelta*>(current_page);

            /* Check if your Key is in merged node  */
            if (reverse_comparator_(key, merge_delta->separator_) > 0) {
              LOG_DEBUG("Found NODE_MERGE_DELTA to insert into");
              current_page = merge_delta->physical_link_;
            } else {
              current_page = current_page->GetDeltaNext();
            }

            continue;
          }
          case LEAF_NODE: {
            LOG_DEBUG("Visit LEAF_NODE");
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
              __attribute__((unused)) int size_data = data_items.size();
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
                    DeallocatePage(new_modify_delta_page);
                    LOG_DEBUG("CAS of consolidation success");

                    /* Check if split is required and perform the operation */
                    if (!Split_Operation(consolidated_page, pages_visited,
                        current_PID)) {
                      /* Check if merge is requires and perform the operation */
                      Merge_Operation(consolidated_page, pages_visited,
                          current_PID);
                    }
                  }
                }
              }
            }
            DeregisterWorker(worker_epoch);
            return inserted;
          }
          case MODIFY_DELTA: {
            LOG_DEBUG("Visit MODIFY_DELTA");
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
                    DeallocatePage(new_modify_delta_page);
                    LOG_DEBUG("CAS of consolidation success");

                    /* Check if split is required and perform the operation */
                    if (!Split_Operation(consolidated_page, pages_visited,
                        current_PID)) {

                      /* Check if merge is requires and perform the operation */
                      Merge_Operation(consolidated_page, pages_visited,
                          current_PID);
                    }
                  }
                }
              }
            }
            DeregisterWorker(worker_epoch);
            return inserted;
          }
          default:
            throw IndexException("Unrecognized page type\n");
            break;
        }
      }
    }
  }
  DeregisterWorker(worker_epoch);
  return false;
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker,
          bool _Duplicates,
          class ValueEqualityChecker>
bool BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, _Duplicates,
            ValueEqualityChecker>::Delete(const KeyType& key,
                                          const ValueType& data) {
  LOG_DEBUG("Trying delete");
  uint64_t worker_epoch = RegisterWorker();
  while (true) {
    Page* root_page = map_table_[root_];

    /* If empty root w/ no children then delete fails */
    if (root_page->GetType() == INNER_NODE &&
        reinterpret_cast<InnerNode*>(root_page)->children_.size() == 0) {
      LOG_DEBUG("Delete false 1");
      DeregisterWorker(worker_epoch);
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
            LOG_DEBUG("Merge SMO completion succeeded with PID %ld into PID %ld", current_PID, remove_delta->merged_into_);
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
          if (reverse_comparator_(key, merge_delta->separator_) > 0) {
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
            DeregisterWorker(worker_epoch);
            return false;
          }

          __attribute__((unused)) int size_data = data_items.size();
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
                DeallocatePage(new_modify_delta_page);
                LOG_DEBUG("CAS of consolidation success");

                LOG_DEBUG("Checking Split Threshold");
                /* Check if split is required and perform the operation */
                if (!Split_Operation(consolidated_page, pages_visited, current_PID)) {

                  LOG_DEBUG("Checking Merge Threshold");
                  /* Check if merge is requires and perform the operation */
                  Merge_Operation(consolidated_page, pages_visited, current_PID);
                }
              }
            }
          }
          DeregisterWorker(worker_epoch);
          return found_location;
        }
        case MODIFY_DELTA: {
          ModifyDelta* mod_delta = reinterpret_cast<ModifyDelta*>(current_page);

          bool found_location = false;
          if (equals_(key, mod_delta->key_)) {
            std::vector<ValueType> data_items;

            /* If empty means nothing to delete */
            if (mod_delta->locations_.size() == 0) {
              DeregisterWorker(worker_epoch);
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
              DeregisterWorker(worker_epoch);
              return false;
            }

            __attribute__((unused)) int size_data = data_items.size();
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
                  DeallocatePage(new_modify_delta_page);

                  LOG_DEBUG("CAS of consolidation success");

                  /* Check if split is required and perform the operation */
                  if (!Split_Operation(consolidated_page, pages_visited,
                      current_PID)) {

                    /* Check if merge is requires and perform the operation */
                    Merge_Operation(consolidated_page, pages_visited,
                        current_PID);
                  }
                }
              }
            }
          } else {
            current_page = current_page->GetDeltaNext();
            continue;
          }
          DeregisterWorker(worker_epoch);
          return found_location;
        }
        default:
          throw IndexException("Unrecognized page type\n");
          break;
      }
    }
  }

  /* Deletion Failed */
  DeregisterWorker(worker_epoch);
  return false;
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker,
          bool _Duplicates,
          class ValueEqualityChecker>
std::vector<ValueType>
BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, _Duplicates,
       ValueEqualityChecker>::SearchKey(__attribute__((unused))
                                        const KeyType& key) {
  Page* current_page = map_table_[root_];
  PID current_PID = root_;
  uint64_t worker_epoch = RegisterWorker();

  /* If empty root w/ no children then search fails */
  if (current_page->GetType() == INNER_NODE &&
      reinterpret_cast<InnerNode*>(current_page)->children_.size() == 0) {
    LOG_DEBUG("SearchKey returning nothing because BWTree is empty");
    DeregisterWorker(worker_epoch);
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

        // Check if the given key is already located in this leaf
        std::vector<ValueType> data_items;
        for (const auto& key_values : leaf->data_items_) {
          if (equals_(key, key_values.first)) {
            DeregisterWorker(worker_epoch);
            return key_values.second;
          }
        }
        return std::vector<ValueType>();
      }
      case MODIFY_DELTA: {
        ModifyDelta* mod_delta = reinterpret_cast<ModifyDelta*>(current_page);
        if (equals_(key, mod_delta->key_)) {
          DeregisterWorker(worker_epoch);
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

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker,
          bool _Duplicates,
          class ValueEqualityChecker>
std::map<KeyType, std::vector<ValueType>, KeyComparator>
BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker,  _Duplicates,
       ValueEqualityChecker>::SearchAllKeys() {
  LOG_DEBUG("Trying searchAllKeys");
  std::map<KeyType, std::vector<ValueType>, KeyComparator> visited_keys(comparator_);

  Page* current_page = map_table_[root_];
  PID current_PID = root_;
  uint64_t worker_epoch = RegisterWorker();

  /* If empty root w/ no children then search fails */
  if (current_page->GetType() == INNER_NODE &&
      reinterpret_cast<InnerNode*>(current_page)->children_.size() == 0) {
    LOG_DEBUG("SearchAllKeys returning nothing because BWTree is empty");
    DeregisterWorker(worker_epoch);
    return visited_keys;
  }

  bool split_indicator = false;
  __attribute__((unused)) bool merge_indicator = false;
  KeyType split_separator = KeyType();
  PID split_next_node;

  __attribute__((unused)) Page* head_of_delta = current_page;
  while (true) {
    switch (current_page->GetType()) {
      case INNER_NODE: {
        InnerNode* inner_node = reinterpret_cast<InnerNode*>(current_page);
        if (inner_node->children_.size() == 0) {
          DeregisterWorker(worker_epoch);
          return visited_keys;
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

          visited_keys.insert(std::make_pair(key_values.first, key_values.second));
        }

        if (split_indicator) {
          current_PID = split_next_node;
        } else {
          if (leaf->next_leaf_ != NullPID)
            current_PID = leaf->next_leaf_;
          else {
            DeregisterWorker(worker_epoch);
            return visited_keys;
          }
        }

        current_page = map_table_[current_PID];
        head_of_delta = current_page;

        split_indicator = false;
        merge_indicator = false;

        break;
      }
      case MODIFY_DELTA: {
        ModifyDelta* mod_delta = reinterpret_cast<ModifyDelta*>(current_page);
        visited_keys.insert(std::make_pair(mod_delta->key_, mod_delta->locations_));

        current_page = current_page->GetDeltaNext();
        continue;
      }
      default:
        throw IndexException("Unrecognized page type\n");
        break;
    }
  }
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker,
          bool _Duplicates,
          class ValueEqualityChecker>
bool BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker,  _Duplicates,
            ValueEqualityChecker>::Split_Operation(Page* consolidated_page,
                                                   std::stack<PID>&
                                                       pages_visited,
                                                   PID orig_pid) {
  bool split_required = false;
  SplitDelta* split_delta = nullptr;
  IndexTermDelta* index_term_delta_for_split = nullptr;
  InnerNode* new_root_node = nullptr;
  PID reinstalled_root = root_;

  /* Check if split required */
  if (consolidated_page->GetType() == INNER_NODE) {
    InnerNode* node_to_split = reinterpret_cast<InnerNode*>(consolidated_page);
    if (node_to_split->children_.size() > SPLIT_SIZE) {
      LOG_DEBUG("Attempting Split");
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

      /* Check if you are splitting the root node */
      // Need to check how we consolidate to make sure we have correct behavior
      if (orig_pid == root_) {
        LOG_DEBUG("Attempting to split the root node");
        /* Copy over the old root node to a new location */
        reinstalled_root = InstallNewMapping(consolidated_page);
        /* Create the new root node */
        new_root_node = new InnerNode();
        new_root_node->absolute_min_ = true;
        new_root_node->absolute_max_ = true;
        new_root_node->children_.push_back(std::make_pair(new_separator_key, reinstalled_root));
        new_root_node->children_.push_back(std::make_pair(new_inner_node->high_key_, new_node_PID));
      } else {
        /* Create the index term delta for the parent */
        index_term_delta_for_split = new IndexTermDelta(
            new_inner_node->low_key_, new_inner_node->high_key_, new_node_PID);
        index_term_delta_for_split->absolute_max_ = new_inner_node->absolute_max_;
      }
    }
  } else if (consolidated_page->GetType() == LEAF_NODE) {
    __attribute__((unused)) LeafNode* node_to_split =
        reinterpret_cast<LeafNode*>(consolidated_page);
    if (node_to_split->data_items_.size() > SPLIT_SIZE) {
      LOG_DEBUG("Attempting Split");
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
    PID pid_to_use = (orig_pid == root_) ? reinstalled_root : orig_pid;
    assert(pid_to_use != root_);
    if (!map_table_[pid_to_use].compare_exchange_strong(consolidated_page,
                                                      split_delta)) {
      delete split_delta;
      if (orig_pid == root_) {
        delete new_root_node;
      } else {
        delete index_term_delta_for_split;
      }

      LOG_DEBUG("CAS of installing delta split failed");
      return true;
    } else {
      LOG_DEBUG("CAS of installing delta split success");

      /* Check if we are splitting the root node */
      if (orig_pid == root_) {
        assert (new_root_node);
        if (!map_table_[root_].compare_exchange_strong(consolidated_page,
                                                              new_root_node)) {
          LOG_DEBUG("CAS of installing new root node failed");
          delete new_root_node;
          return true;
        }
        LOG_DEBUG("Installing new root node successful");
        return true;
      } else {
        /* Get PID of parent node */
        PID pid_of_parent = pages_visited.top();

        /* Attempt to install index term delta on the parent */
        while (true) {
          Page* parent_node = map_table_[pid_of_parent];
          if (parent_node->GetType() != REMOVE_NODE_DELTA) {
            index_term_delta_for_split->SetDeltaNext(parent_node);
            if (map_table_[pid_of_parent].compare_exchange_strong(
                parent_node, index_term_delta_for_split)) {
              LOG_DEBUG("CAS of installing index term delta in parent succeeded");
              LOG_DEBUG("with low key: %s high key: %s",
                index_term_delta_for_split->low_separator_.GetTupleForComparison(key_tuple_schema).GetInfo().c_str(),
                index_term_delta_for_split->high_separator_.GetTupleForComparison(key_tuple_schema).GetInfo().c_str());
              return true;
            }
          } else {
            delete index_term_delta_for_split;
            LOG_DEBUG("CAS of installing index term delta in parent failed");
            return true;
          }
        }
      }
    }
  }
  return true;
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker,
          bool _Duplicates,
          class ValueEqualityChecker>
bool BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker,  _Duplicates,
            ValueEqualityChecker>::complete_the_split(PID side_link,
                                                      std::stack<PID>&
                                                          pages_visited) {
  Page* new_split_node = map_table_[side_link];
  IndexTermDelta* index_term_delta_for_split = nullptr;

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
    LOG_DEBUG("Neither inner nor leaf node?");
    return false;
  }

  /* Get PID of parent node */
  PID pid_of_parent = pages_visited.top();

  /* Attempt to install index term delta on the parent */
  while (true) {
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
          class KeyEqualityChecker,
          bool _Duplicates,
          class ValueEqualityChecker>
void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, _Duplicates,
            ValueEqualityChecker>::Merge_Operation(Page* consolidated_page,
                                                   std::stack<PID>&
                                                       pages_visited,
                                                   PID orig_pid) {
  bool merge_required = false;
  RemoveNodeDelta* remove_node_delta;
  NodeMergeDelta* merge_delta;
  IndexTermDelta* index_term_delta_for_merge;
  PID pid_merging_into;
  Page* page_merging_into;

  /* Check if merge required */
  if (consolidated_page->GetType() == INNER_NODE) {
    InnerNode* node_to_merge = reinterpret_cast<InnerNode*>(consolidated_page);
    if ((node_to_merge->children_.size() < MERGE_SIZE) &&
        (!node_to_merge->absolute_min_)) {
      LOG_DEBUG("Attempting Merge");
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
      if (current_top_of_page->GetType() == REMOVE_NODE_DELTA) {
        LOG_DEBUG("Merge abandoned - problem w/ left sibling");
        return;
      }
      assert(current_top_of_page->GetType() != REMOVE_NODE_DELTA);
      page_merging_into = Consolidate(pid_merging_into);

      if (!page_merging_into) {
        return;
      }

      /* Create new remove node delta */
      remove_node_delta = new RemoveNodeDelta(pid_merging_into);

      /* Create a new merge delta */
      merge_delta =
          new NodeMergeDelta(node_to_merge->low_key_,
                             node_to_merge->absolute_max_, consolidated_page);

      InnerNode* node_merging_into =
          reinterpret_cast<InnerNode*>(page_merging_into);

      if (node_merging_into->side_link_ != orig_pid) {
        LOG_DEBUG("Abandoning Merge - problem w/ left sibling");
        delete page_merging_into;
        delete remove_node_delta;
        delete merge_delta;
        return;
      }
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
        //FreeDeltaChain(current_top_of_page);
        DeallocatePage(current_top_of_page);
        LOG_DEBUG(
            "Successfully installed consolidated page we are merging into");
      } else {
        delete index_term_delta_for_merge;
        delete page_merging_into;
        delete remove_node_delta;
        delete merge_delta;
        LOG_DEBUG("Abandoning merge - failed to install consolidated page");
        return;
      }
    }
  } else if (consolidated_page->GetType() == LEAF_NODE) {
    __attribute__((unused)) LeafNode* node_to_merge =
        reinterpret_cast<LeafNode*>(consolidated_page);
    if ((node_to_merge->data_items_.size() < MERGE_SIZE) &&
        (!node_to_merge->absolute_min_)) {
      LOG_DEBUG("Attempting Merge");
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
      if (current_top_of_page->GetType() == REMOVE_NODE_DELTA) {
        LOG_DEBUG("Merge abandoned - problem w/ left sibling");
        return;
      }
      assert(current_top_of_page->GetType() != REMOVE_NODE_DELTA);
      page_merging_into = Consolidate(pid_merging_into);

      if (!page_merging_into) {
        return;
      }

      /* Create new remove node delta */
      remove_node_delta = new RemoveNodeDelta(pid_merging_into);

      /* Create a new merge delta */
      merge_delta =
          new NodeMergeDelta(node_to_merge->low_key_,
                             node_to_merge->absolute_max_, consolidated_page);

      __attribute__((unused)) LeafNode* node_merging_into =
          reinterpret_cast<LeafNode*>(page_merging_into);

      if (node_merging_into->side_link_ != orig_pid) {
        LOG_DEBUG("Abandoning Merge - problem w/ left sibling");
        delete page_merging_into;
        delete remove_node_delta;
        delete merge_delta;
        return;
      }
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
        DeallocatePage(current_top_of_page);
        LOG_DEBUG(
            "Successfully installed consolidated page we are merging into");
      } else {
        delete index_term_delta_for_merge;
        delete page_merging_into;
        delete remove_node_delta;
        delete merge_delta;
        LOG_DEBUG("Abandoning merge - failed to install consolidated page");
        return;
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
      LOG_DEBUG("CAS of installing remove node success with PID: %ld", orig_pid);

      /* Attempt to install node merge delta */
      if (page_merging_into->GetType() == REMOVE_NODE_DELTA) {
        delete merge_delta;
        delete index_term_delta_for_merge;
        LOG_DEBUG("CAS of installing node merge delta failed");
        return;
      } else {
        merge_delta->SetDeltaNext(page_merging_into);
        if (map_table_[pid_merging_into].compare_exchange_strong(
            page_merging_into, merge_delta)) {
          LOG_DEBUG("CAS of installing node merge delta succeeded with PID: %ld", pid_merging_into);
          LOG_DEBUG("with separator key: %s",
                merge_delta->separator_.GetTupleForComparison(key_tuple_schema).GetInfo().c_str());
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
        Page* parent_node = map_table_[pid_of_parent];
        if (parent_node->GetType() != REMOVE_NODE_DELTA) {
          index_term_delta_for_merge->SetDeltaNext(parent_node);
          if (map_table_[pid_of_parent].compare_exchange_strong(
                  parent_node, index_term_delta_for_merge)) {
            LOG_DEBUG("CAS of installing index term delta in parent succeeded with PID: %ld", index_term_delta_for_merge->side_link_);
            LOG_DEBUG("with low key: %s high key: %s",
                index_term_delta_for_merge->low_separator_.GetTupleForComparison(key_tuple_schema).GetInfo().c_str(),
                index_term_delta_for_merge->high_separator_.GetTupleForComparison(key_tuple_schema).GetInfo().c_str());
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

/* Returns root PID if we fail to find child */
template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker,
          bool _Duplicates,
          class ValueEqualityChecker>
std::uint64_t
BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker,  _Duplicates,
       ValueEqualityChecker>::find_left_sibling(std::stack<PID>& pages_visited,
                                                KeyType merge_key) {
  PID parent_pid = pages_visited.top();
  Page* parent_page = Consolidate(parent_pid);
  if (!parent_page) {
    return root_;
  }

  LOG_DEBUG("Find left sibling for key: %s", merge_key.GetTupleForComparison(key_tuple_schema).GetInfo().c_str());

  /* Go over the delta chain looking for node neighboring the merge_key */
  while (true) {
    switch (parent_page->GetType()) {
      case INNER_NODE: {
        InnerNode* inner_node = reinterpret_cast<InnerNode*>(parent_page);

        for (const auto& child : inner_node->children_) {
          if (reverse_comparator_(merge_key, child.first) == 0) {
            LOG_DEBUG("Found left sibling in Inner Node");
            PID found_pid = child.second;
            delete parent_page;
            return found_pid;
          }
        }

        /* Couldn't find neighbor */
        delete parent_page;
        return root_;

        break;
      }
      case INDEX_TERM_DELTA: {
        assert(0);
        IndexTermDelta* idx_delta =
            reinterpret_cast<IndexTermDelta*>(parent_page);

        if (reverse_comparator_(merge_key, idx_delta->high_separator_) == 0) {
          LOG_DEBUG("Found left sibling in Index Term Delta");
          return idx_delta->side_link_;
        } else {
          parent_page = parent_page->GetDeltaNext();
        }
        continue;
      }
      case SPLIT_DELTA: {
        assert(0);
        /* If parent is split abandon merge */
        // if split higher try more
        return root_;
        break;
      }
      case REMOVE_NODE_DELTA: {
        assert(0);
        /* If parent is deleted abandon merge */
        return root_;
        break;
      }
      case NODE_MERGE_DELTA: {
        assert(0);
        /* If parent is merged abandon merge */
        return root_;
        break;
      }
      case LEAF_NODE: {
        assert(0);
        break;
      }
      case MODIFY_DELTA: {
        assert(0);
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
          class KeyEqualityChecker,
          bool _Duplicates,
          class ValueEqualityChecker>
bool BWTree<
    KeyType, ValueType, KeyComparator, KeyEqualityChecker, _Duplicates,
    ValueEqualityChecker>::complete_the_merge(RemoveNodeDelta* remove_node,
                                              std::stack<PID>& pages_visited) {
  PID merged_into_pid = remove_node->merged_into_;
  IndexTermDelta* index_term_delta = nullptr;

  /* Get the high key of the page that's deleted */
  Page* page_deleted = remove_node->GetDeltaNext();

  /* Consolidated the page we are merging into */
  Page* page_merging_into = map_table_[merged_into_pid];
  Page* page_merging_into_consolidate = Consolidate(merged_into_pid);

  if (!page_merging_into_consolidate) {
    LOG_DEBUG("Failed to consolidated page merging into");
    return false;
  }


  /* Install consolidated page being merged into */
  if (!map_table_[merged_into_pid].compare_exchange_strong(
          page_merging_into, page_merging_into_consolidate)) {
    delete page_merging_into_consolidate;
    LOG_DEBUG("CAS of consolidated page failed");
    return false;
  } else {
    DeallocatePage(page_merging_into);
  }

  /* Check if merge delta is present */
  if (page_deleted->GetType() == INNER_NODE) {
    InnerNode* inner_node = reinterpret_cast<InnerNode*>(page_deleted);
    if (!is_merge_installed(page_merging_into_consolidate, page_deleted,
                            inner_node->high_key_)) {
      LOG_DEBUG("Need to install merge delta for incomplete SMO");

      /* Create a new merge delta */
      NodeMergeDelta* merge_delta = new NodeMergeDelta(
          inner_node->low_key_, inner_node->absolute_max_, page_deleted);

      /* Install Merge Delta */
      merge_delta->SetDeltaNext(page_merging_into_consolidate);
      if (!map_table_[merged_into_pid].compare_exchange_strong(
              page_merging_into_consolidate, merge_delta)) {
        LOG_DEBUG("CAS of merge delta failed");
        delete merge_delta;
        return false;
      }
    }

    InnerNode* inner_merged_into =
        reinterpret_cast<InnerNode*>(page_merging_into_consolidate);
    index_term_delta =
        new IndexTermDelta(inner_merged_into->low_key_,
                           inner_node->high_key_, merged_into_pid);
    index_term_delta->absolute_max_ = inner_node->absolute_max_;
    index_term_delta->absolute_min_ = inner_merged_into->absolute_min_;
  } else if (page_deleted->GetType() == LEAF_NODE) {
    __attribute__((unused)) LeafNode* leaf =
        reinterpret_cast<LeafNode*>(page_deleted);
    if (!is_merge_installed(page_merging_into_consolidate, page_deleted,
                            leaf->high_key_)) {
      LOG_DEBUG("Need to install merge delta for incomplete SMO");

      /* Create a new merge delta */
      NodeMergeDelta* merge_delta = new NodeMergeDelta(
          leaf->low_key_, leaf->absolute_max_, page_deleted);

      /* Install Merge Delta */
      merge_delta->SetDeltaNext(page_merging_into_consolidate);
      if (!map_table_[merged_into_pid].compare_exchange_strong(
              page_merging_into_consolidate, merge_delta)) {
        LOG_DEBUG("CAS of merge delta failed");
        delete merge_delta;
        return false;
      }
    }

    __attribute__((unused)) LeafNode* leaf_merged_into =
        reinterpret_cast<LeafNode*>(page_merging_into_consolidate);
    index_term_delta =
        new IndexTermDelta(leaf_merged_into->low_key_,
                           leaf->high_key_, merged_into_pid);
    index_term_delta->absolute_max_ = leaf->absolute_max_;
    index_term_delta->absolute_min_ = leaf_merged_into->absolute_min_;
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
          class KeyEqualityChecker,
          bool _Duplicates,
          class ValueEqualityChecker>
bool BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker,  _Duplicates,
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
          LOG_DEBUG("because of this?");
          return true;
        }

        current_page = current_page->GetDeltaNext();
        continue;
      }
      case LEAF_NODE: {
        __attribute__((unused)) LeafNode* leaf =
            reinterpret_cast<LeafNode*>(current_page);

        if (reverse_comparator_(high_key, leaf->high_key_) <= 0) {
          LOG_DEBUG("because of that?");
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

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker,
          bool _Duplicates,
          class ValueEqualityChecker>
void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker,  _Duplicates,
            ValueEqualityChecker>::RunEpochManager() {
  if (!GC_ENABLED) {
    assert(false);
  }
  std::mutex mtx;
  std::unique_lock<std::mutex> lck(mtx);
  while (exec_finished_.wait_for(lck,
        std::chrono::milliseconds(EPOCH_INTERVAL_MS)) == std::cv_status::timeout
      && !finished_) {
    LOG_DEBUG("Timed out...");
    // Get next epoch #
    uint64_t next_epoch = epoch_ + 1;
    LOG_DEBUG("Incrementing epoch to %u", (unsigned) next_epoch);

    // Add new epoch slots to the active workers map and the garbage
    // collection map
    //gc_lock.WriteLock();
    LOG_DEBUG("epoch garbage size: %u", (unsigned) epoch_garbage_.size());
    LOG_DEBUG("active threads size: %u", (unsigned) active_threads_map_.size());
    active_threads_map_[next_epoch] = 0;
    epoch_garbage_[next_epoch] = std::vector<Page*>();

    // Now increment the global epoch counter. This should always be
    // equal to next_epoch because this thread should be the only one
    // ever changing it.
    ++epoch_;
    assert(epoch_ == next_epoch);
    for (uint64_t i = 0; i < epoch_; ++i) {
      auto entry = active_threads_map_.find(i);
      if (entry != active_threads_map_.end() && active_threads_map_[i] > 0) {
        LOG_DEBUG("Epoch %u has %u active threads remaining",(unsigned) i,
            (unsigned)active_threads_map_[i]);
      }
    }
    if (finished_)
      break;
  }
  LOG_DEBUG("Woken up, exiting...");
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker,
          bool _Duplicates,
          class ValueEqualityChecker>
uint64_t BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, _Duplicates,
            ValueEqualityChecker>::RegisterWorker() {
  if (!GC_ENABLED) {
    return 0;
  }
  uint64_t current_epoch = epoch_;
  LOG_DEBUG("Registering thread for epoch %u", (unsigned) current_epoch);
  ++(active_threads_map_[current_epoch]);
  return current_epoch;
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker,
          bool _Duplicates ,
          class ValueEqualityChecker>
void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, _Duplicates,
            ValueEqualityChecker>::DeregisterWorker(uint64_t worker_epoch) {
  if (!GC_ENABLED) {
    return;
  }
  LOG_DEBUG("Deregistering thread from epoch %u", (unsigned) worker_epoch);
  --(active_threads_map_[worker_epoch]);
  assert(active_threads_map_[worker_epoch] >= 0);
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker,
          bool _Duplicates,
          class ValueEqualityChecker>
void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, _Duplicates,
            ValueEqualityChecker>::DeallocatePage(Page *page) {
  dealloc_lock.WriteLock();
  uint64_t current_epoch = epoch_;
  LOG_DEBUG("Deallocating page in epoch %u", (unsigned) current_epoch);
  epoch_garbage_[current_epoch].push_back(page);
  dealloc_lock.Unlock();
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker,
          bool _Duplicates,
          class ValueEqualityChecker>
bool BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, _Duplicates,
            ValueEqualityChecker>::Cleanup() {
  if (!GC_ENABLED) {
    return true;
  }
  LOG_DEBUG("Cleaning up old pages");
  cleanup_lock.WriteLock();
  uint64_t current_epoch = epoch_;

  // Find all epochs that are safe to garbage collect
  for (uint64_t i = 0; i < current_epoch; ++i) {
    auto entry = active_threads_map_.find(i);
    if (entry != active_threads_map_.end() && entry->second == 0) {
      // This is a valid epoch and there are no active threads remaining

      LOG_DEBUG("Found safe epoch %u", (unsigned) i);
      std::vector<Page*>& dealloc_pages = epoch_garbage_[i];
      LOG_DEBUG("Deleting garbage (%u items) in safe epoch %u\n", (unsigned) dealloc_pages.size(),
              (unsigned) i);
      for (uint64_t j = 0; j < dealloc_pages.size(); ++j) {
        Page *chain_head = dealloc_pages[j];
        FreeDeltaChain(chain_head);
        dealloc_pages[j] = nullptr;
      }
      active_threads_map_.erase(i);
      epoch_garbage_.erase(i);
    }
  }
  LOG_DEBUG("epoch garbage size: %u", (unsigned) epoch_garbage_.size());
  LOG_DEBUG("active threads size: %u", (unsigned) active_threads_map_.size());
  assert(epoch_garbage_.size() == active_threads_map_.size());
  cleanup_lock.Unlock();

  return true;
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker,
          bool _Duplicates,
          class ValueEqualityChecker>
size_t BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, _Duplicates,
            ValueEqualityChecker>::GetMemoryFootprint() {
  if (!GC_ENABLED) {
    return 0;
  }
  LOG_DEBUG("Starting GetMemoryFootprint");
  uint64_t worker_epoch = RegisterWorker();
  size_t memory_footprint = 0;

  // First walk through mapping table and calculate footprint
  PID num_pids = PID_counter_;
  for (PID i = 0; i < PID_counter_; ++i) {
    Page *current_page = map_table_[i];
    while (current_page != nullptr) {
      memory_footprint += GetPageSize(current_page);
      current_page = current_page->GetDeltaNext();
    }
  }

  // Now walk through pages to be deallocated
  uint64_t current_epoch = epoch_;
  cleanup_lock.ReadLock();

  for (uint64_t i = 0; i < current_epoch; ++i) {
    auto entry = epoch_garbage_.find(i);
    if (entry != epoch_garbage_.end()) {
      std::vector<Page*> chain_heads = entry->second;
      for (Page *chain_head : chain_heads) {
        Page *current_page = chain_head;
        while (current_page != nullptr) {
          memory_footprint += GetPageSize(current_page);
          current_page = current_page->GetDeltaNext();
        }
      }
    }
  }
  cleanup_lock.Unlock();

  DeregisterWorker(worker_epoch);
  LOG_DEBUG("Finished GetMemoryFootprint");
  return memory_footprint;
}

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker,
          bool _Duplicates,
          class ValueEqualityChecker>
size_t BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, _Duplicates,
            ValueEqualityChecker>::GetPageSize(Page *page) {
  size_t page_size = 0;
  switch (page->GetType()) {
    case INNER_NODE: {
      InnerNode* inner_node = reinterpret_cast<InnerNode*>(page);
      page_size = sizeof(*inner_node);
      break;
    }
    case INDEX_TERM_DELTA: {
      IndexTermDelta* index_delta = reinterpret_cast<IndexTermDelta*>(page);
      page_size = sizeof(*index_delta);
      break;
    }
    case SPLIT_DELTA: {
      SplitDelta* split_delta = reinterpret_cast<SplitDelta*>(page);
      page_size = sizeof(*split_delta);
      break;
    }
    case REMOVE_NODE_DELTA: {
      RemoveNodeDelta* remove_delta = reinterpret_cast<RemoveNodeDelta*>(page);
      page_size = sizeof(*remove_delta);
      break;
    }
    case NODE_MERGE_DELTA: {
      NodeMergeDelta* merge_delta = reinterpret_cast<NodeMergeDelta*>(page);
      page_size = sizeof(*merge_delta);
      break;
    }
    case LEAF_NODE: {
      LeafNode* leaf = reinterpret_cast<LeafNode*>(page);
      page_size = sizeof(*leaf);
      break;
    }
    case MODIFY_DELTA: {
      ModifyDelta* modify_delta = reinterpret_cast<ModifyDelta*>(page);
      page_size = sizeof(*modify_delta);
      break;
    }
    default:
      page_size = 64;
      break;
  }
  assert(page_size > 0);
  return page_size;
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
