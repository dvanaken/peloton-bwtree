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
