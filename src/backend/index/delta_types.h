#pragma once

/*
 * These will probably all become child classes of the node class
 * just putting it here for now
 */


enum delta_type_t {
  RECORD_INSERT,
  RECORD_MODIFY,
  RECORD_DELETE,
  SPLIT,
  INDEX_INSERT,
  REMOVE_NODE,
  MERGE,
  INDEX_DELETE
};

class Delta_Record {
public:
  delta_type_t delta_type;
};

/* Record Modifications */
template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
class Delta_Record_Insert: public Delta_Record {
public:
  delta_type_t delta_type;
  KeyType record_id;
  ItemPointer location;

  Delta_Record_Insert(KeyType record_id, ItemPointer location) :
    delta_type(RECORD_INSERT), record_id(record_id), location(location) {}
};

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
class Delta_Record_Modify: public Delta_Record {
public:
  delta_type_t delta_type;
  KeyType record_id;
  ItemPointer location;

  Delta_Record_Modify(KeyType record_id, ItemPointer location) :
      delta_type(RECORD_MODIFY), record_id(record_id), location(location) {}
};

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
class Delta_Record_Delete: public Delta_Record {
public:
  delta_type_t delta_type;
  KeyType record_id;

  Delta_Record_Delete(KeyType record_id) :
        delta_type(RECORD_DELETE), record_id(record_id) {}
};

/* Split SMO */
template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
class Delta_Record_Split: public Delta_Record {
public:
  delta_type_t delta_type;
  KeyType separator_key;
  uint64_t new_sibling;

  Delta_Record_Split(KeyType separator_key, uint64_t new_sibling) :
    delta_type(SPLIT), separator_key(separator_key), new_sibling(new_sibling) {}
};

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
class Delta_Record_Index_Insert: public Delta_Record {
public:
  delta_type_t delta_type;
  KeyType separator_key_old_child;
  uint64_t new_child;
  KeyType separator_key_new_child;

  Delta_Record_Index_Insert(KeyType separator_key_old_child, uint64_t new_child, KeyType separator_key_new_child) :
    delta_type(INDEX_INSERT), separator_key_old_child(separator_key_old_child), new_child(new_child),
    separator_key_new_child(separator_key_new_child) {}
};

/* Merge SMO */
class Delta_Record_Delete: public Delta_Record {
public:
  delta_type_t delta_type;

  Delta_Record_Delete(): delta_type(REMOVE_NODE) {}
};

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
class Delta_Record_Merge: public Delta_Record {
public:
  delta_type_t delta_type;
  KeyType separator_key;
  void* deleting_node;

  Delta_Record_Merge(KeyType separator_key, void* deleting_node) :
    delta_type(MERGE), separator_key(separator_key), deleting_node(deleting_node) {}
};

template <typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
class Delta_Record_Index_Delete: public Delta_Record {
public:
  delta_type_t delta_type;
  uint64_t child_deleting;
  uint64_t merged_into;
  KeyType low_key;
  KeyType high_key;

  Delta_Record_Index_Delete(KeyType separator_key, uint64_t child_deleting, uint64_t merged_into,
      KeyType low_key, KeyType high_key) :
    delta_type(INDEX_DELETE), child_deleting(child_deleting), merged_into(merged_into),
    low_key(low_key), high_key(high_key) {}
};


// Storing unused code ...
/*
template <typename KeyType, typename ValueType, class KeyComparator,
class KeyEqualityChecker, class ValueEqualityChecker>
bool BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker,
ValueEqualityChecker>::is_left_sibling(
    PID this_pid, PID orig_pid) {
  Page * current_page = map_table_[this_pid];

  // Go over delta chain and check if this is the left sibling
  while (true) {
    switch(current_page->GetType()) {
    case INNER_NODE: {
      InnerNode* inner_node = reinterpret_cast<InnerNode*>(current_page);

      if (inner_node->absolute_max_) {
        return false;
      }

      if (inner_node->side_link_ == orig_pid) {
        return true;
      } else {
        return false;
      }
      break;
    }
    case INDEX_TERM_DELTA: {
      current_page = current_page->GetDeltaNext();
      break;
    }
    case SPLIT_DELTA: {
      SplitDelta* split_delta =
          reinterpret_cast<SplitDelta*>(current_page);

      //TODO: Need to consider this, might cause incorrect behavior (aaron)
      if (split_delta->side_link_ == orig_pid) {
        return true;
      } else {
        return false;
      }
      break;
    }
    case REMOVE_NODE_DELTA:
      return false;
      break;
    }
    case NODE_MERGE_DELTA: {
      // Go down  the path of the node that was deleted
      NodeMergeDelta * merge_delta =
          reinterpret_cast<NodeMergeDelta*>(current_page);

      current_page = merge_delta->physical_link_;

      if (current_page->GetType() == InnerNode) {
        InnerNode* inner_node = reinterpret_cast<InnerNode*>(consolidated_page);
        if (inner_node->side_link_ == orig_pid) {
          return true;
        } else {
          return false;
        }
      } else if (consolidated_page->GetType() == LEAF_NODE) {
        __attribute__((unused)) LeafNode* leaf_node =
            reinterpret_cast<LeafNode*>(consolidated_page);
        if (leaf_node->side_link_ == orig_pid) {
          return true;
        } else {
          return false;
        }
      } else {
        assert(0);
      }
      break;
    }
    case LEAF_NODE: {
      __attribute__((unused)) LeafNode* leaf_node =
          reinterpret_cast<LeafNode*>(consolidated_page);
      if (leaf_node->side_link_ == orig_pid) {
        return true;
      } else {
        return false;
      }
      break;
    }
    case MODIFY_DELTA: {
      current_page = current_page->GetDeltaNext();
      break;
    }
    default:
      throw IndexException("Unrecognized page type\n");
      break;
  }

  assert(0);
  return false;



  template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker, class ValueEqualityChecker>
KeyType BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker,
ValueEqualityChecker>::find_low_range(Page * head_of_chain,
    bool & is_absolute_min, KeyType high_key) {
  Page * current_page = head_of_chain;
  bool set_low_range = false;
  KeyType new_low_key = high_key;

  while (true) {
    switch(current_page->GetType()) {
    case INNER_NODE: {
      InnerNode* inner_node = reinterpret_cast<InnerNode*>(current_page);

      if (!set_low_range) {
        is_absolute_min = inner_node->absolute_min_;
        assert(comparator_(inner_node->low_key_, new_low_key) < 0);
        return inner_node->low_key_;
      } else {
        if (inner_node->absolute_min_) {
          is_absolute_min = true;
        }
        if (comparator_(inner_node->low_key_, new_low_key) < 0) {
          *new_low_key = inner_node->low_key_;
        }
        return *new_low_key;
      }

      break;
    }
    case INDEX_TERM_DELTA: {
      IndexTermDelta* idx_delta =
          reinterpret_cast<IndexTermDelta*>(current_page);

      if (!set_low_range) {
        assert(comparator_(idx_delta->low_separator_, new_low_key) < 0);
        new_low_key = idx_delta->low_separator_;
        is_absolute_min = idx_delta->absolute_min_;
      } else {
        if (inner_node->absolute_min_) {
          is_absolute_min = true;
        }
        if (comparator_(idx_delta->low_separator_, new_low_key) < 0) {
          new_low_key = idx_delta->low_separator_;
        }
        set_low_range = true;
      }
      current_page = current_page->GetDeltaNext();

      continue;
    }
    case SPLIT_DELTA: {
      return high_key;
      break;
    }
    case REMOVE_NODE_DELTA: {
      // If we encounter deleted abandon merge
      return high_key;
      break;
    }
    case NODE_MERGE_DELTA: {
      current_page = current_page->GetDeltaNext();
      break;
    }
    case LEAF_NODE: {
      __attribute__((unused)) LeafNode* leaf =
          reinterpret_cast<LeafNode*>(current_page);

      if (!set_low_range) {
        is_absolute_min = leaf->absolute_min_;
        assert(comparator_(leaf->low_key_, new_low_key) < 0);
        return leaf->low_key_;
      } else {
        if (leaf->absolute_min_) {
          is_absolute_min = true;
        }
        if (comparator_(leaf->low_key_, new_low_key) < 0) {
          *new_low_key = leaf->low_key_;
        }
        return *new_low_key;
      }
      break;
    }
    case MODIFY_DELTA: {
      ModifyDelta* mod_delta = reinterpret_cast<ModifyDelta*>(current_page);

      current_page = current_page->GetDeltaNext();
      break;
    }
    default:
      throw IndexException("Unrecognized page type\n");
      break;
    }
  }
}

} */
