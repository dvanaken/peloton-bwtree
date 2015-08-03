/*-------------------------------------------------------------------------
 *
 * index_test.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/tests/index/index_test.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "gtest/gtest.h"
#include "harness.h"

#include "backend/common/logger.h"
#include "backend/index/index_factory.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Index Tests
//===--------------------------------------------------------------------===//

void PrintSlots(const std::vector<ItemPointer>& slots) {

  std::cout << "SLOTS :: " << slots.size() << "\n";
  for(auto item : slots)
    std::cout << item.block << " " << item.offset << "\n";

}

TEST(IndexTests, BtreeUniqueIndexTest) {
  std::vector<std::vector<std::string> > column_names;
  std::vector<catalog::Column> columns;
  std::vector<catalog::Schema *> schemas;

  // SCHEMA

  catalog::Column column1(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                          "A", true);
  catalog::Column column2(VALUE_TYPE_VARCHAR, 25, "B", true);
  catalog::Column column3(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                          "C", true);
  catalog::Column column4(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                          "D", true);

  columns.push_back(column1);
  columns.push_back(column2);

  catalog::Schema *key_schema = new catalog::Schema(columns);
  key_schema->SetIndexedColumns({0, 1});

  columns.push_back(column3);
  columns.push_back(column4);

  catalog::Schema *tuple_schema = new catalog::Schema(columns);

  // BTREE UNIQUE INDEX
  bool unique_keys = true;

  index::IndexMetadata *index_metadata = new index::IndexMetadata(
      "btree_index", 125, INDEX_TYPE_BTREE,
      INDEX_CONSTRAINT_TYPE_DEFAULT, tuple_schema, key_schema, unique_keys);

  storage::VMBackend *backend = new storage::VMBackend();
  peloton::Pool *pool = new peloton::Pool(backend);

  index::Index *index = index::IndexFactory::GetInstance(index_metadata);

  EXPECT_EQ(true, index != NULL);

  // INDEX

  storage::Tuple *key0 = new storage::Tuple(key_schema, true);
  storage::Tuple *key1 = new storage::Tuple(key_schema, true);
  storage::Tuple *key2 = new storage::Tuple(key_schema, true);
  storage::Tuple *key3 = new storage::Tuple(key_schema, true);
  storage::Tuple *key4 = new storage::Tuple(key_schema, true);
  storage::Tuple *keynonce = new storage::Tuple(key_schema, true);

  key0->SetValue(0, ValueFactory::GetIntegerValue(100));
  key1->SetValue(0, ValueFactory::GetIntegerValue(100));
  key2->SetValue(0, ValueFactory::GetIntegerValue(100));
  key3->SetValue(0, ValueFactory::GetIntegerValue(400));
  key4->SetValue(0, ValueFactory::GetIntegerValue(500));
  keynonce->SetValue(0, ValueFactory::GetIntegerValue(1000));

  key0->SetValue(1, ValueFactory::GetStringValue("a", pool));
  key1->SetValue(1, ValueFactory::GetStringValue("b", pool));
  key2->SetValue(1, ValueFactory::GetStringValue("c", pool));
  key3->SetValue(1, ValueFactory::GetStringValue("d", pool));
  key4->SetValue(1, ValueFactory::GetStringValue("e", pool));
  keynonce->SetValue(1, ValueFactory::GetStringValue("f", pool));

  ItemPointer item0(120, 5);
  ItemPointer item1(120, 7);
  ItemPointer item2(123, 19);

  index->InsertEntry(key0, item0);
  index->InsertEntry(key1, item1);
  index->InsertEntry(key1, item2);
  index->InsertEntry(key2, item1);
  index->InsertEntry(key3, item1);
  index->InsertEntry(key4, item1);

  LOG_TRACE("Scan \n");
  auto slots = index->Scan();
  EXPECT_EQ(slots.size(), 5);

  auto location = index->Exists(keynonce, INVALID_ITEMPOINTER);
  EXPECT_EQ(location.block, INVALID_OID);
  location = index->Exists(key0, INVALID_ITEMPOINTER);
  EXPECT_EQ(location.block, item0.block);

  LOG_TRACE("Key \n");
  slots = index->GetLocationsForKey(key1);
  EXPECT_EQ(slots.size(), 1);

  LOG_TRACE("Key \n");
  slots = index->GetLocationsForKey(key0);
  EXPECT_EQ(slots.size(), 1);

  LOG_TRACE("Key Between \n");
  slots = index->GetLocationsForKeyBetween(key1, key4);
  EXPECT_EQ(slots.size(), 2);

  LOG_TRACE("Key LT \n");
  slots = index->GetLocationsForKeyLT(key3);
  EXPECT_EQ(slots.size(), 3);

  LOG_TRACE("Key LTE \n");
  slots = index->GetLocationsForKeyLTE(key3);
  EXPECT_EQ(slots.size(), 4);

  LOG_TRACE("Key GT \n");
  slots = index->GetLocationsForKeyGT(key1);
  EXPECT_EQ(slots.size(), 3);

  LOG_TRACE("Key GTE \n");
  slots = index->GetLocationsForKeyGTE(key1);
  EXPECT_EQ(slots.size(), 4);

  LOG_TRACE("Delete \n");

  index->DeleteEntry(key0, INVALID_ITEMPOINTER);
  index->DeleteEntry(key1, INVALID_ITEMPOINTER);
  index->DeleteEntry(key2, INVALID_ITEMPOINTER);
  index->DeleteEntry(key3, INVALID_ITEMPOINTER);
  index->DeleteEntry(key4, INVALID_ITEMPOINTER);

  location = index->Exists(key0, INVALID_ITEMPOINTER);
  EXPECT_EQ(location.block, INVALID_OID);

  LOG_TRACE("Scan \n");
  index->Scan();
  slots = index->Scan();
  EXPECT_EQ(slots.size(), 0);

  delete key0;
  delete key1;
  delete key2;
  delete key3;
  delete key4;
  delete keynonce;

  delete pool;
  delete backend;

  delete tuple_schema;

  delete index;
}

TEST(IndexTests, BtreeMultiIndexTest) {
  std::vector<std::vector<std::string> > column_names;
  std::vector<catalog::Column> columns;
  std::vector<catalog::Schema *> schemas;

  // SCHEMA

  catalog::Column column1(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                          "A", true);
  catalog::Column column2(VALUE_TYPE_VARCHAR, 25, "B", true);
  catalog::Column column3(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                          "C", true);
  catalog::Column column4(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                          "D", true);

  columns.push_back(column1);
  columns.push_back(column2);

  catalog::Schema *key_schema = new catalog::Schema(columns);
  key_schema->SetIndexedColumns({0, 1});

  columns.push_back(column3);
  columns.push_back(column4);

  catalog::Schema *tuple_schema = new catalog::Schema(columns);

  // BTREE MULTI INDEX

  bool unique_keys = false;
  index::IndexMetadata *index_metadata = new index::IndexMetadata(
      "btree_index", 125, INDEX_TYPE_BTREE,
      INDEX_CONSTRAINT_TYPE_DEFAULT, tuple_schema, key_schema, unique_keys);

  storage::VMBackend *backend = new storage::VMBackend();
  peloton::Pool *pool = new peloton::Pool(backend);

  index::Index *index = index::IndexFactory::GetInstance(index_metadata);

  EXPECT_EQ(true, index != NULL);

  // INDEX

  storage::Tuple *key0 = new storage::Tuple(key_schema, true);
  storage::Tuple *key1 = new storage::Tuple(key_schema, true);
  storage::Tuple *key2 = new storage::Tuple(key_schema, true);
  storage::Tuple *key3 = new storage::Tuple(key_schema, true);
  storage::Tuple *key4 = new storage::Tuple(key_schema, true);
  storage::Tuple *keynonce = new storage::Tuple(key_schema, true);

  key0->SetValue(0, ValueFactory::GetIntegerValue(100));
  key1->SetValue(0, ValueFactory::GetIntegerValue(100));
  key2->SetValue(0, ValueFactory::GetIntegerValue(100));
  key3->SetValue(0, ValueFactory::GetIntegerValue(400));
  key4->SetValue(0, ValueFactory::GetIntegerValue(500));
  keynonce->SetValue(0, ValueFactory::GetIntegerValue(1000));

  key0->SetValue(1, ValueFactory::GetStringValue("a", pool));
  key1->SetValue(1, ValueFactory::GetStringValue("b", pool));
  key2->SetValue(1, ValueFactory::GetStringValue("c", pool));
  key3->SetValue(1, ValueFactory::GetStringValue("d", pool));
  key4->SetValue(1, ValueFactory::GetStringValue("e", pool));
  keynonce->SetValue(1, ValueFactory::GetStringValue("f", pool));

  ItemPointer item0(120, 5);
  ItemPointer item1(120, 7);
  ItemPointer item2(123, 19);

  index->InsertEntry(key0, item0);
  index->InsertEntry(key1, item1);
  index->InsertEntry(key1, item2);
  index->InsertEntry(key2, item1);
  index->InsertEntry(key3, item1);
  index->InsertEntry(key4, item1);

  LOG_TRACE("Scan \n");
  auto slots = index->Scan();
  EXPECT_EQ(slots.size(), 6);

  auto location = index->Exists(keynonce, INVALID_ITEMPOINTER);
  EXPECT_EQ(location.block, INVALID_OID);
  location = index->Exists(key0, item0);
  EXPECT_EQ(location.block, item0.block);

  LOG_TRACE("Key \n");
  slots = index->GetLocationsForKey(key1);
  EXPECT_EQ(slots.size(), 2);

  LOG_TRACE("Key \n");
  slots = index->GetLocationsForKey(key0);
  EXPECT_EQ(slots.size(), 1);

  LOG_TRACE("Key Between \n");
  slots = index->GetLocationsForKeyBetween(key1, key4);
  EXPECT_EQ(slots.size(), 2);

  LOG_TRACE("Key LT \n");
  slots = index->GetLocationsForKeyLT(key3);
  EXPECT_EQ(slots.size(), 4);

  LOG_TRACE("Key LTE \n");
  slots = index->GetLocationsForKeyLTE(key3);
  EXPECT_EQ(slots.size(), 5);

  LOG_TRACE("Key GT \n");
  slots = index->GetLocationsForKeyGT(key1);
  EXPECT_EQ(slots.size(), 3);

  LOG_TRACE("Key GTE \n");
  slots = index->GetLocationsForKeyGTE(key1);
  EXPECT_EQ(slots.size(), 5);

  LOG_TRACE("Delete \n");

  index->DeleteEntry(key0, item0);
  index->DeleteEntry(key1, item1);
  index->DeleteEntry(key1, item2);
  index->DeleteEntry(key2, item1);
  index->DeleteEntry(key3, item1);
  index->DeleteEntry(key4, item1);

  location = index->Exists(key0, INVALID_ITEMPOINTER);
  EXPECT_EQ(location.block, INVALID_OID);

  LOG_TRACE("Scan \n");
  index->Scan();
  slots = index->Scan();
  EXPECT_EQ(slots.size(), 1);

  delete key0;
  delete key1;
  delete key2;
  delete key3;
  delete key4;
  delete keynonce;

  delete pool;
  delete backend;

  delete tuple_schema;

  delete index;
}

}  // End test namespace
}  // End peloton namespace
