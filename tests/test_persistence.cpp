// #include "include.hpp"

#include <gtest/gtest.h>

#include <memory>

#include "arrow_help.hpp"
#include "output_util.hpp"



// 测试简单的读写RecordBatch
// 把RecordBatch写入磁盘，并读取制定的列
TEST(Persistence, ArrowToParquet) {
  // 创建一个简单的 RecordBatch
  std::vector<int> int_builder = {1, 2, 3};
  std::shared_ptr<arrow::Array> int_array;
  DaseX::Util::AppendIntArray(int_array, int_builder);

  std::vector<std::string> str_builder = {"x", "y", "z"};
  std::shared_ptr<arrow::Array> str_array;
  DaseX::Util::AppendStringArray(str_array, str_builder);

  auto schema = arrow::schema({
      arrow::field("int_col", arrow::int32()),
      arrow::field("str_col", arrow::utf8()),
      arrow::field("str_col2", arrow::utf8())
  });

  auto batch = arrow::RecordBatch::Make(schema, 3, {int_array, str_array, str_array});

  // 写入 Parquet 文件
  DaseX::Util::WriteRecordBatchToDisk(batch, "example.parquet");

  // 从 Parquet 文件读取
  std::shared_ptr<arrow::RecordBatch> batch_read;
  std::vector<int> column_indices = {0, 2};
  DaseX::Util::ReadRecordBatchFromDisk("example.parquet", column_indices, schema, batch_read);

  DaseX::DisplayChunk(batch_read);

}