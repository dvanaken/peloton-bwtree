//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// data_table_test.cpp
//
// Identification: tests/storage/data_table_test.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "gtest/gtest.h"

#include "backend/storage/data_table.h"
#include "backend/storage/tile_group.h"
#include "executor/executor_tests_util.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Data Table Tests
//===--------------------------------------------------------------------===//

TEST(DataTableTests, TransformTileGroupTest) {
  const int tuples_per_tile_group = TESTS_TUPLES_PER_TILEGROUP;
  const int tile_group_count = 5;
  const int tuple_count = tuples_per_tile_group * tile_group_count;

  // Create a table and wrap it in logical tiles
  std::unique_ptr<storage::DataTable> data_table(
      ExecutorTestsUtil::CreateTable(tuples_per_tile_group, false));
  ExecutorTestsUtil::PopulateTable(data_table.get(), tuple_count,
                                   false, false, true);

  // Create the new column map
  storage::column_map_type column_map;
  column_map[0] = std::make_pair(0, 0);
  column_map[1] = std::make_pair(0, 1);
  column_map[2] = std::make_pair(1, 0);
  column_map[3] = std::make_pair(1, 1);

  auto theta = 0.0;

  // Transform the tile group
  data_table->TransformTileGroup(0, theta);

  // Create the another column map
  column_map[0] = std::make_pair(0, 0);
  column_map[1] = std::make_pair(0, 1);
  column_map[2] = std::make_pair(0, 2);
  column_map[3] = std::make_pair(1, 0);

  // Transform the tile group
  data_table->TransformTileGroup(0, theta);

  // Create the another column map
  column_map[0] = std::make_pair(0, 0);
  column_map[1] = std::make_pair(1, 0);
  column_map[2] = std::make_pair(1, 1);
  column_map[3] = std::make_pair(1, 2);

  // Transform the tile group
  data_table->TransformTileGroup(0, theta);

  // Update column map stats
  data_table->UpdateColumnMapStats();

  // Print column map stats
  data_table->PrintColumnMapStats();

}

}  // End test namespace
}  // End peloton namespace
