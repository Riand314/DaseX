#include "import_data.hpp"
#include "file_dir_config.hpp"
// =============================Q15=============================
// 测试前需要先生成Revenue数据
// 测试单线程Q15查询
TEST(TPCHTest, Q15SingleThreadTest) {
    std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
    bool importFinish = false;
    std::string fileDir = fileDir1;
    // 插入Revenue表数据
    std::shared_ptr<Table> table_revenue;
    InsertRevenue(table_revenue, scheduler, fileDir, importFinish);
    importFinish = false;
    spdlog::info("[{} : {}] InsertRevenue Finish!!!", __FILE__, __LINE__);

    // 插入Supplier表数据
    std::shared_ptr<Table> table_supplier;
    InsertSupplier(table_supplier, scheduler, fileDir, importFinish);
    importFinish = false;
    spdlog::info("[{} : {}] InsertSupplier Finish!!!", __FILE__, __LINE__);

    //  ================================================构建Scan================================================
    std::vector<int> revenue_ids1 = {1};
    // <total_revenue>
    std::shared_ptr<PhysicalTableScan> scan_revenue1= std::make_shared<PhysicalTableScan>(table_revenue, -1, revenue_ids1);

    std::vector<int> revenue_ids2 = {0, 1};
    // <supplier_no, total_revenue>
    std::shared_ptr<PhysicalTableScan> scan_revenue2= std::make_shared<PhysicalTableScan>(table_revenue, -1, revenue_ids2);

    std::vector<int> supplier_ids = {0, 1, 2, 4};
    // <s_suppkey, s_name, s_address, s_phone>
    std::shared_ptr<PhysicalTableScan> scan_supplier = std::make_shared<PhysicalTableScan>(table_supplier, -1, supplier_ids);

    //  ================================================构建HashAgg================================================
    std::vector<AggFunctionType> aggregate_function_types = { AggFunctionType::MAX};
    std::vector<int32_t> group_item_idxs = {0};
    // <Max(total_revenue)>
    std::shared_ptr<PhysicalHashAgg> hashAgg1 = std::make_shared<PhysicalHashAgg>(-1, aggregate_function_types, group_item_idxs);

    //  ================================================构建HashJoin================================================
    std::vector<int> build_ids1 = {0};
    std::vector<int> prode_ids1 = {1};
    // <Max(total_revenue), supplier_no, total_revenue>
    std::shared_ptr<PhysicalHashJoin> join1 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids1, prode_ids1);
    join1->exp_name = "1";

    std::vector<int> build_ids2 = {0};
    std::vector<int> prode_ids2 = {1};
    // <s_suppkey, s_name, s_address, s_phone, Max(total_revenue), supplier_no, total_revenue>
    std::shared_ptr<PhysicalHashJoin> join2 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids2, prode_ids2);
    join2->exp_name = "2";

    //  ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p1_s_suppkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 0, 0);
    std::shared_ptr<BoundColumnRefExpression> p1_s_name = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    std::shared_ptr<BoundColumnRefExpression> p1_s_address = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 2, 0);
    std::shared_ptr<BoundColumnRefExpression> p1_s_phone = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 3, 0);
    std::shared_ptr<BoundColumnRefExpression> p1_total_revenue = std::make_shared<BoundColumnRefExpression>(LogicalType::DOUBLE, 0, 6, 0);
    std::vector<std::shared_ptr<Expression>> p1_expressions1 = {p1_s_suppkey, p1_s_name, p1_s_address, p1_s_phone, p1_total_revenue};
    // < s_suppkey, s_name, s_address, s_phone, total_revenue>
    std::shared_ptr<PhysicalProject> project1 = std::make_shared<PhysicalProject>(p1_expressions1);
    project1->exp_name = "1";
    //  ================================================构建OrderBy================================================
    std::vector<int> sort_keys = {0};
    std::vector<LogicalType> key_types = {LogicalType::INTEGER};
    std::vector<SortOrder> orders = {SortOrder::ASCENDING};
    std::shared_ptr<PhysicalOrder> orderby = std::make_shared<PhysicalOrder>(&sort_keys, &key_types, &orders);

    orderby->AddChild(project1);
    project1->AddChild(join2);
    join2->AddChild(scan_supplier);
    join2->AddChild(join1);
    join1->AddChild(hashAgg1);
    join1->AddChild(scan_revenue2);
    hashAgg1->AddChild(scan_revenue1);

    std::shared_ptr<Pipeline> pipeline = std::make_shared<Pipeline>();
    std::shared_ptr<PipelineGroup> pipeline_group = std::make_shared<PipelineGroup>();
    orderby->build_pipelines(pipeline, pipeline_group);
    std::shared_ptr<PipelineGroupExecute> pipeline_group_executor = std::make_shared<PipelineGroupExecute>(scheduler, pipeline_group);
    pipeline_group_executor->traverse_plan();
    auto start_execute = std::chrono::steady_clock::now();
    pipeline_group_executor->execute();
    auto end_execute = std::chrono::steady_clock::now();
    spdlog::info("[{} : {}] Q15执行时间: {}", __FILE__, __LINE__, Util::time_difference(start_execute, end_execute));

    // 关闭线程池
    scheduler->shutdown();
    RC expected = RC::SUCCESS;
    ASSERT_EQ(expected, RC::SUCCESS);
}

// 测试4线程Q15查询
TEST(TPCHTest, Q15FourThreadTest) {
    std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
    bool importFinish = false;
    std::string fileDir = fileDir4;
    std::vector<int> work_ids(arr, arr + sizeof(arr) / sizeof(arr[0]));
    std::vector<int> partition_idxs(arr, arr + sizeof(arr) / sizeof(arr[0]));
    // 插入Revenue表数据
    std::shared_ptr<Table> table_revenue;
    std::vector<std::string> revenue_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "revenue.tbl_" + std::to_string(i);
        revenue_file_names.emplace_back(file_name);
    }
    InsertRevenueMul(table_revenue, scheduler, work_ids, partition_idxs, revenue_file_names, importFinish);
    importFinish = false;
    // spdlog::info("[{} : {}] InsertRevenue Finish!!!", __FILE__, __LINE__);

    // 插入Supplier表数据
    std::shared_ptr<Table> table_supplier;
    std::vector<std::string> supplier_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "supplier.tbl_" + std::to_string(i);
        supplier_file_names.emplace_back(file_name);
    }
    InsertSupplierMul(table_supplier, scheduler, work_ids, partition_idxs, supplier_file_names, importFinish);
    importFinish = false;
    std::this_thread::sleep_for(std::chrono::seconds(1));
    // spdlog::info("[{} : {}] InsertSupplier Finish!!!", __FILE__, __LINE__);

    std::cout << "Q15: SELECT\n"
                 "    s_suppkey,\n"
                 "    s_name,\n"
                 "    s_address,\n"
                 "    s_phone,\n"
                 "    total_revenue\n"
                 "FROM\n"
                 "    supplier,\n"
                 "    revenue\n"
                 "WHERE\n"
                 "    s_suppkey = supplier_no\n"
                 "    AND total_revenue = (\n"
                 "        SELECT\n"
                 "            MAX(total_revenue)\n"
                 "        FROM\n"
                 "            revenue\n"
                 "    )\n"
                 "ORDER BY\n"
                 "    s_suppkey;" << std::endl;

    //  ================================================构建Scan================================================
    std::vector<int> revenue_ids1 = {1};
    // <total_revenue>
    std::shared_ptr<PhysicalTableScan> scan_revenue1= std::make_shared<PhysicalTableScan>(table_revenue, -1, revenue_ids1);

    std::vector<int> revenue_ids2 = {0, 1};
    // <supplier_no, total_revenue>
    std::shared_ptr<PhysicalTableScan> scan_revenue2= std::make_shared<PhysicalTableScan>(table_revenue, -1, revenue_ids2);

    std::vector<int> supplier_ids = {0, 1, 2, 4};
    // <s_suppkey, s_name, s_address, s_phone>
    std::shared_ptr<PhysicalTableScan> scan_supplier = std::make_shared<PhysicalTableScan>(table_supplier, -1, supplier_ids);

    //  ================================================构建HashAgg================================================
    std::vector<AggFunctionType> aggregate_function_types = { AggFunctionType::MAX};
    std::vector<int32_t> group_item_idxs = {0};
    // <Max(total_revenue)>
    std::shared_ptr<PhysicalHashAgg> hashAgg1 = std::make_shared<PhysicalHashAgg>(-1, aggregate_function_types, group_item_idxs);

    //  ================================================构建HashJoin================================================
    std::vector<int> build_ids1 = {0};
    std::vector<int> prode_ids1 = {1};
    // <Max(total_revenue), supplier_no, total_revenue>
    std::shared_ptr<PhysicalHashJoin> join1 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids1, prode_ids1);
    join1->exp_name = "1";

    std::vector<int> build_ids2 = {0};
    std::vector<int> prode_ids2 = {1};
    // <s_suppkey, s_name, s_address, s_phone, Max(total_revenue), supplier_no, total_revenue>
    std::shared_ptr<PhysicalHashJoin> join2 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids2, prode_ids2);
    join2->exp_name = "2";

    //  ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p1_s_suppkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 0, 0);
    std::shared_ptr<BoundColumnRefExpression> p1_s_name = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    std::shared_ptr<BoundColumnRefExpression> p1_s_address = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 2, 0);
    std::shared_ptr<BoundColumnRefExpression> p1_s_phone = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 3, 0);
    std::shared_ptr<BoundColumnRefExpression> p1_total_revenue = std::make_shared<BoundColumnRefExpression>(LogicalType::DOUBLE, 0, 6, 0);
    std::vector<std::shared_ptr<Expression>> p1_expressions1 = {p1_s_suppkey, p1_s_name, p1_s_address, p1_s_phone, p1_total_revenue};
    // < s_suppkey, s_name, s_address, s_phone, total_revenue>
    std::shared_ptr<PhysicalProject> project1 = std::make_shared<PhysicalProject>(p1_expressions1);
    project1->exp_name = "1";
    //  ================================================构建OrderBy================================================
    std::vector<int> sort_keys = {0};
    std::vector<LogicalType> key_types = {LogicalType::INTEGER};
    std::vector<SortOrder> orders = {SortOrder::ASCENDING};
    std::shared_ptr<PhysicalOrder> orderby = std::make_shared<PhysicalOrder>(&sort_keys, &key_types, &orders);

    orderby->AddChild(project1);
    project1->AddChild(join2);
    join2->AddChild(scan_supplier);
    join2->AddChild(join1);
    join1->AddChild(hashAgg1);
    join1->AddChild(scan_revenue2);
    hashAgg1->AddChild(scan_revenue1);

    std::shared_ptr<Pipeline> pipeline = std::make_shared<Pipeline>();
    std::shared_ptr<PipelineGroup> pipeline_group = std::make_shared<PipelineGroup>();
    orderby->build_pipelines(pipeline, pipeline_group);
    std::shared_ptr<PipelineGroupExecute> pipeline_group_executor = std::make_shared<PipelineGroupExecute>(scheduler, pipeline_group);
    pipeline_group_executor->traverse_plan();
    pipeline_group_executor->execute();
    spdlog::info("[{} : {}] Q15执行时间: {} ms.", __FILE__, __LINE__, pipeline_group_executor->duration.count());
    std::string executeTime = std::to_string(pipeline_group_executor->duration.count()) + " ms.";
    std::ofstream env_file("/home/cwl/workspace/dasex/executeTime.txt", std::ios::trunc);
    if (env_file) {
        env_file << executeTime << std::endl;
        env_file.close();
    } else {
        std::cerr << "Failed to write to /etc/environment. Try running as root." << std::endl;
    }
    // 关闭线程池
    scheduler->shutdown();
    RC expected = RC::SUCCESS;
    ASSERT_EQ(expected, RC::SUCCESS);
}