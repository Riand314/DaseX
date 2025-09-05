#include "create_table.hpp"
#include "query_handler.hpp"
#include "import_data.hpp"

using namespace DaseX;

#define TEST_TPCH false

namespace {
const int arr[] = {1, 2, 3, 4};
const std::string fileDir4 = "../test_data/4thread";
const std::string fileDir1 = "../test_data";

std::string fileDir = fileDir4;
std::vector<int> work_ids(arr, arr + sizeof(arr) / sizeof(arr[0]));
std::vector<int> partition_idxs(arr, arr + sizeof(arr) / sizeof(arr[0]));

}

#if TEST_TPCH
TEST(TPCHBySqlsTest, TPCHBySqlsTest) {
	std::string test_sql_tpch_q1 = 
		"select "
		"l_returnflag, "
		"l_linestatus, "
		"sum(l_quantity) as sum_qty, "
		"sum(l_extendedprice) as sum_base_price,  "
        "sum(l_extendedprice*(1-l_discount)) as sum_disc_price, "
        "sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge, "
		"avg(l_quantity) as avg_qty, "
		"avg(l_extendedprice) as avg_price, "
		"avg(l_discount) as avg_disc, "
		"count(*) as count_order "
		"from "
			"lineitem "
		"where "
			"l_shipdate <= 875635200 "
		"group by  "
			"l_returnflag, l_linestatus "
		"order by "
			"l_returnflag, l_linestatus;";

	
	std::string database_path = "temp_database_all_multi";
  std::shared_ptr<DaseX::CataLog> catalog(nullptr);

  // 打开数据库
  ASSERT_EQ(OpenDatabase(database_path, catalog, true),  RC::SUCCESS);
  ASSERT_NE(catalog.get(), nullptr);

  // Query
	QueryHandler query_handler(catalog);
	auto start_execute = std::chrono::steady_clock::now();
	ASSERT_EQ(query_handler.Query(test_sql_tpch_q1), RC::SUCCESS);
	auto end_execute = std::chrono::steady_clock::now();

  query_handler.result_temp.print_result();

	spdlog::info("[{} : {}] Q1执行时间: {}", __FILE__, __LINE__, Util::time_difference(start_execute, end_execute));
}

#else


TEST(LoadFromFile, LoadSinglePartitionAllTable) {
  auto load_single = [](std::string table_name, 
    std::function<DaseX::TableInfo()> &create_table_info,
    std::shared_ptr<DaseX::CataLog> &catalog) {

    int partition_idx = 0;
    std::shared_ptr<Table> table;
    std::string file_name = fileDir1 + "/" + table_name + ".tbl";

    // 创建表
    auto table_info = create_table_info();
    ASSERT_EQ(catalog->register_table(table_info), RC::SUCCESS);
    ASSERT_EQ(catalog->get_table(table_info.table_name, table), RC::SUCCESS);
    
    // 加载数据
    spdlog::info("[{}:{}] Start load '{}' from '{}'", __FILE__, __LINE__, table_info.table_name, file_name);
    auto rc = catalog->load_from_single_file(table_info.table_name, table, partition_idx, 
      file_name, CHUNK_SIZE, '|');
    ASSERT_EQ(rc, RC::SUCCESS);
    spdlog::info("[{}:{}] End load '{}'", __FILE__, __LINE__, table_info.table_name);

    // 读取数据
    std::string test_sql = "SELECT * FROM " + table_info.table_name + ";";
    QueryHandler query_handler(catalog);
    rc = query_handler.Query(test_sql);
    auto result_org = query_handler.result_temp.result_chunks;

    ASSERT_EQ(query_handler.result_temp.get_row_count(), table->row_nums);
    return;
  };

  std::vector<std::string> file_names = {
    "customer",
    "lineitem",
    "nation",
    "orders",
    "partsupp",
    "part",
    "region",
    "revenue",
    "supplier",
  };
  std::vector<std::function<DaseX::TableInfo()>>  functionArray = {
    CreateTableCustomerInfo,
    CreateTableLineitemInfo,
    CreateTableNationInfo,
    CreateTableOrdersInfo,
    CreateTablePartsuppInfo,
    CreateTablePartInfo,
    CreateTableRegionInfo,
    CreateTableRevenueInfo,
    CreateTableSupplierInfo,
  };

  std::string database_path = "temp_database_all";
  std::shared_ptr<DaseX::CataLog> catalog(nullptr);
  std::shared_ptr<Table> table;

  // 创建数据库
  DeleteDatabase(database_path);
  ASSERT_EQ(OpenDatabase(database_path, catalog, true),  RC::SUCCESS);
  ASSERT_NE(catalog.get(), nullptr);

  for(int i = 0; i < file_names.size(); i++) {
    load_single(file_names[i], functionArray[i], catalog);
    // break;
  }
}

TEST(LoadFromFile, LoadMultiPartition) {
	auto load_multi = [](std::string table_name,
						 std::function<DaseX::TableInfo()> &create_table_info,
						 std::shared_ptr<DaseX::CataLog> &catalog) {
		std::shared_ptr<Table> table;
		std::vector<std::string> file_names;
		for (int i = 0; i < work_ids.size(); i++) {
			std::string file_name =
				fileDir4 + "/" + table_name + ".tbl_" + std::to_string(i);
			file_names.emplace_back(file_name);
		}

		// 创建表
		auto table_info = create_table_info();
		ASSERT_EQ(catalog->register_table(table_info), RC::SUCCESS);
		ASSERT_EQ(catalog->get_table(table_info.table_name, table),
				  RC::SUCCESS);

		// 加载数据
		spdlog::info("[{}:{}] Start load '{}'", __FILE__, __LINE__,
					 table_info.table_name);
		auto rc = catalog->load_from_files(table_info.table_name, table,
										   work_ids, partition_idxs, file_names,
										   CHUNK_SIZE, '|');
		ASSERT_EQ(rc, RC::SUCCESS);
		spdlog::info("[{}:{}] End load '{}'", __FILE__, __LINE__,
					 table_info.table_name);

		// 读取数据
		std::string test_sql = "SELECT * FROM " + table_info.table_name + ";";
		QueryHandler query_handler(catalog);
		rc = query_handler.Query(test_sql);
		auto result_org = query_handler.result_temp.result_chunks;

		for (int i = 0; i < partition_idxs.size(); i++) {
			spdlog::info("partition {} row_nums: {}", i,
						 table->partition_tables[partition_idxs[i]]->row_nums);
		}
		ASSERT_EQ(RC::SUCCESS, rc);
    // 打印所有partition的row_num
    for(int i = 0; i < partition_idxs.size(); i++) {
      spdlog::info("partition {} row_nums: {}", i, table->partition_tables[partition_idxs[i]]->row_nums);
    }
		// FIXME: 这里因为没有对结果汇总，一个partition对上了就算对了
		ASSERT_EQ(query_handler.result_temp.get_row_count(),
				  table->partition_tables[partition_idxs.front()]->row_nums);
	};

	std::vector<std::string> file_names = {
		"customer", "lineitem", "nation",  "orders",   "partsupp",
		"part",		"region",	"revenue", "supplier",
	};
	std::vector<std::function<DaseX::TableInfo()> > functionArray = {
		CreateTableCustomerInfo, CreateTableLineitemInfo,
		CreateTableNationInfo,	 CreateTableOrdersInfo,
		CreateTablePartsuppInfo, CreateTablePartInfo,
		CreateTableRegionInfo,	 CreateTableRevenueInfo,
		CreateTableSupplierInfo,
	};

	std::string database_path = "temp_database_all_multi";
	std::shared_ptr<DaseX::CataLog> catalog(nullptr);
	std::shared_ptr<Table> table;

	// 创建数据库
	DeleteDatabase(database_path);
	ASSERT_EQ(OpenDatabase(database_path, catalog, true), RC::SUCCESS);
	ASSERT_NE(catalog.get(), nullptr);

	for (int i = 0; i < file_names.size(); i++) {
		load_multi(file_names[i], functionArray[i], catalog);
		// break;
	}
}

// 测试数据库能不能正常创建，并在路径非法时创建失败
TEST(Metadata, CreateDeleteDatabase) {
  std::string database_path = "temp_database";
  std::shared_ptr<DaseX::CataLog> catalog;
  DeleteDatabase(database_path);

  ASSERT_EQ(OpenDatabase(database_path, catalog, false), RC::IO_ERROR);
  ASSERT_EQ(catalog.get(), nullptr);

  ASSERT_EQ(OpenDatabase(database_path, catalog, true),  RC::SUCCESS);
  ASSERT_NE(catalog.get(), nullptr);
  ASSERT_EQ(catalog->database_path_, database_path);
  catalog.reset();

  ASSERT_EQ(OpenDatabase(database_path, catalog, false), RC::SUCCESS);
  ASSERT_NE(catalog.get(), nullptr);
  ASSERT_EQ(catalog->database_path_, database_path);
  catalog.reset();

  ASSERT_EQ(DeleteDatabase(database_path), RC::SUCCESS);
  ASSERT_EQ(OpenDatabase(database_path, catalog, false), RC::IO_ERROR);

  ASSERT_EQ(OpenDatabase(database_path, catalog, true),  RC::SUCCESS);
  ASSERT_EQ(DeleteDatabase(database_path), RC::SUCCESS);
}

// 测试数据库表能不能正常创建，删除
TEST(Metadata, CreateAndDeleteTable) {
  std::string database_path = "temp_database";
  std::shared_ptr<DaseX::CataLog> catalog(nullptr);
  std::shared_ptr<Table> table;
  DeleteDatabase(database_path);
  ASSERT_EQ(OpenDatabase(database_path, catalog, true),  RC::SUCCESS);
  ASSERT_NE(catalog.get(), nullptr);

  auto table_info = CreateTableCustomerInfo();
  ASSERT_EQ(catalog->register_table(table_info), RC::SUCCESS);
  ASSERT_EQ(catalog->register_table(table_info), RC::TABLE_ALREADY_EXISTS);
  ASSERT_EQ(catalog->get_table(table_info.table_name, table), RC::SUCCESS);

  // 重新打开依然能正常读取数据库
  table.reset();
  catalog.reset();
  ASSERT_EQ(OpenDatabase(database_path, catalog, false),  RC::SUCCESS);
  ASSERT_EQ(catalog->register_table(table_info), RC::TABLE_ALREADY_EXISTS);
  ASSERT_EQ(catalog->get_table(table_info.table_name, table), RC::SUCCESS);

  table.reset();
  catalog.reset();

  DeleteDatabase(database_path);
}

// 创建数据库并把导入数据持久化写入磁盘，打开后读取依然能正确读取数据
TEST(TestLoadAndRead, LoadAndRead) {
  std::string database_path = "temp_database";
  std::shared_ptr<DaseX::CataLog> catalog(nullptr);
  std::shared_ptr<Table> table;

  // 创建数据库
  DeleteDatabase(database_path);
  ASSERT_EQ(OpenDatabase(database_path, catalog, true),  RC::SUCCESS);
  ASSERT_NE(catalog.get(), nullptr);

  // 创建表
  auto table_info = CreateTableCustomerInfo();
  ASSERT_EQ(catalog->register_table(table_info), RC::SUCCESS);
  ASSERT_EQ(catalog->get_table(table_info.table_name, table), RC::SUCCESS);

  // 加载数据
  spdlog::info("[{}:{}] Start load {}", __FILE__, __LINE__, table_info.table_name);
  int partition_idx = 0;
  std::string file_name = fileDir1 + "/" + table_info.table_name + ".tbl";
  auto rc = catalog->load_from_single_file(table_info.table_name, table, partition_idx, 
    file_name, CHUNK_SIZE, '|');
  ASSERT_EQ(rc, RC::SUCCESS);
  spdlog::info("[{}:{}] End load {}", __FILE__, __LINE__, table_info.table_name);

  // 读取数据
  std::string test_sql = "SELECT * FROM " + table_info.table_name + ";";
  QueryHandler query_handler(catalog);
  rc = query_handler.Query(test_sql);
  auto result_org = query_handler.result_temp.result_chunks;
  // query_handler.result_temp.print_result();

  // 重启数据库
  table.reset();
  catalog.reset();
  ASSERT_EQ(OpenDatabase(database_path, catalog, true),  RC::SUCCESS);
  ASSERT_NE(catalog.get(), nullptr);
  ASSERT_EQ(catalog->get_table(table_info.table_name, table), RC::SUCCESS);

  // 读取数据
  QueryHandler query_handler2(catalog);
  rc = query_handler2.Query(test_sql);
  auto result_read = query_handler2.result_temp.result_chunks;
  
  ASSERT_TRUE(Util::AreRecordBatchVectorsEqual(result_org, result_read, true));
}

// 测试读取LoadSinglePartitionAllTable方法导入数据库temp_database_all的数据
TEST(ReadFromDatabase, SinglePartition) {
  auto read_single = [](std::string table_name, 
    std::function<DaseX::TableInfo()> &create_table_info,
    std::shared_ptr<DaseX::CataLog> &catalog) {

    int partition_idx = 0;
    std::shared_ptr<Table> table;

    // 打开表
    ASSERT_EQ(catalog->get_table(table_name, table), RC::SUCCESS);
    

    // 读取数据
    std::string test_sql = "SELECT * FROM " + table_name + ";";
    QueryHandler query_handler(catalog);
    auto rc = query_handler.Query(test_sql);
    auto result_org = query_handler.result_temp.result_chunks;

    ASSERT_EQ(query_handler.result_temp.get_row_count(), table->row_nums);
    return;
  };

  std::vector<std::string> file_names = {
    "customer",
    "lineitem",
    "nation",
    "orders",
    "partsupp",
    "part",
    "region",
    "revenue",
    "supplier",
  };
  std::vector<std::function<DaseX::TableInfo()>>  functionArray = {
    CreateTableCustomerInfo,
    CreateTableLineitemInfo,
    CreateTableNationInfo,
    CreateTableOrdersInfo,
    CreateTablePartsuppInfo,
    CreateTablePartInfo,
    CreateTableRegionInfo,
    CreateTableRevenueInfo,
    CreateTableSupplierInfo,
  };

  std::string database_path = "temp_database_all";
  std::shared_ptr<DaseX::CataLog> catalog(nullptr);

  // 创建数据库
  LoadSinglePartitionAllTable(database_path, true);

  // 打开数据库
  ASSERT_EQ(OpenDatabase(database_path, catalog, true),  RC::SUCCESS);
  ASSERT_NE(catalog.get(), nullptr);

  for(int i = 0; i < file_names.size(); i++) {
    read_single(file_names[i], functionArray[i], catalog);
    // break;
  }
}

TEST(ReadFromDatabase, MultiPartition) {
	auto read_multi = [](std::string table_name,
						 std::function<DaseX::TableInfo()> &create_table_info,
						 std::shared_ptr<DaseX::CataLog> &catalog) {
		std::shared_ptr<Table> table;
		std::vector<std::string> file_names;
		for (int i = 0; i < work_ids.size(); i++) {
			std::string file_name =
				fileDir4 + "/" + table_name + ".tbl_" + std::to_string(i);
			file_names.emplace_back(file_name);
		}

		// 打开表
		ASSERT_EQ(catalog->get_table(table_name, table), RC::SUCCESS);
    // 打印所有partition的row_num
    for(int i = 0; i < partition_idxs.size(); i++) {
      spdlog::info("partition {} row_nums: {}", partition_idxs[i], table->partition_tables[partition_idxs[i]]->row_nums);
    }

		// 读取数据
		std::string test_sql = "SELECT * FROM " + table_name + ";";
		QueryHandler query_handler(catalog);
		auto rc = query_handler.Query(test_sql);
		auto result_org = query_handler.result_temp.result_chunks;
		ASSERT_EQ(RC::SUCCESS, rc);

    // 打印所有partition的row_num
    for(int i = 0; i < partition_idxs.size(); i++) {
      spdlog::info("partition {} row_nums: {}", partition_idxs[i], table->partition_tables[partition_idxs[i]]->row_nums);
    }
		// FIXME: 这里因为没有对结果汇总，一个partition对上了就算对了
		ASSERT_EQ(query_handler.result_temp.get_row_count(),
				  table->partition_tables[partition_idxs.front()]->row_nums);
    
	};

	std::vector<std::string> file_names = {
		"customer", "lineitem", "nation",  "orders",   "partsupp",
		"part",		"region",	"revenue", "supplier",
	};
	std::vector<std::function<DaseX::TableInfo()> > functionArray = {
		CreateTableCustomerInfo, CreateTableLineitemInfo,
		CreateTableNationInfo,	 CreateTableOrdersInfo,
		CreateTablePartsuppInfo, CreateTablePartInfo,
		CreateTableRegionInfo,	 CreateTableRevenueInfo,
		CreateTableSupplierInfo,
	};

	std::string database_path = "temp_database_all_multi";
	std::shared_ptr<DaseX::CataLog> catalog(nullptr);

  // 创建数据库
  LoadMultiPartitionAllTable(database_path, true);

	// 打开数据库
	ASSERT_EQ(OpenDatabase(database_path, catalog, true), RC::SUCCESS);
	ASSERT_NE(catalog.get(), nullptr);

	for (int i = 0; i < file_names.size(); i++) {
		read_multi(file_names[i], functionArray[i], catalog);
		// break;
	}
}


#endif