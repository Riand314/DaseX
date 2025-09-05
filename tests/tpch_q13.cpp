#include "import_data.hpp"
#include "file_dir_config.hpp"
// =============================Q13=============================
// 注意修改数据文件路径
TEST(TPCHTest, Q13SingleThreadTest) {
    std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
    bool importFinish = false;
    std::string fileDir = fileDir1;
    // 插入Customer表数据
    std::shared_ptr<Table> table_customer;
    InsertCustomer(table_customer, scheduler, fileDir, importFinish);
    importFinish = false;
    spdlog::info("[{} : {}] InsertCustomer Finish!!!", __FILE__, __LINE__);
    // 插入Orders表数据
    std::shared_ptr<Table> table_orders;
    InsertOrders(table_orders, scheduler, fileDir, importFinish);
    spdlog::info("[{} : {}] InsertOrders Finish!!!", __FILE__, __LINE__);
    Util::print_socket_free_memory();
    //  ================================================构建Scan================================================
    std::vector<int> orders_ids = {0, 1, 8};
    // <o_orderkey, o_custkey, o_comment>
    std::shared_ptr<PhysicalTableScan> scan_orders = std::make_shared<PhysicalTableScan>(table_orders, -1, orders_ids);
    // <c_custkey>
    std::vector<int> customer_ids = {0};
    std::shared_ptr<PhysicalTableScan> scan_customer = std::make_shared<PhysicalTableScan>(table_customer,  -1, customer_ids);

    //  ================================================构建Filter================================================
    // 构建Filter表达式  o_comment NOT LIKE '%pending%deposits%'
    std::shared_ptr<BoundColumnRefExpression> left_comment = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 2, 0);
    std::string like_pattern = "%pending%deposits%";
    ScalarFunction bound_function = NotLikeFun::GetNotLikeFunction();
    Value value_pattern(like_pattern);
    std::shared_ptr<BoundConstantExpression> right_comment = std::make_shared<BoundConstantExpression>(value_pattern);
    std::vector<std::shared_ptr<Expression>> arguments;
    arguments.push_back(std::move(left_comment));
    arguments.push_back(std::move(right_comment));
    std::shared_ptr<BoundFunctionExpression> functionExpression = std::make_shared<BoundFunctionExpression>(LogicalType::BOOL, bound_function, arguments, nullptr);
    functionExpression->bind_info = functionExpression->function.bind(functionExpression->function, functionExpression->children);
    // <o_orderkey, o_custkey, o_comment>
    std::shared_ptr<PhysicalFilter> filter = std::make_shared<PhysicalFilter>(functionExpression);

    //  ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p1_custkey_col = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 1, 0);
    std::shared_ptr<BoundColumnRefExpression> p1_orderkey_col = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 0, 0);
    std::vector<std::shared_ptr<Expression>> expressions = {p1_custkey_col, p1_orderkey_col};
    // <o_custkey, o_orderkey>
    std::shared_ptr<PhysicalProject> project1 = std::make_shared<PhysicalProject>(expressions);

    //  ================================================构建HashJoin_LEFT================================================
    std::vector<int> build_ids = {0};
    std::vector<int> prode_ids = {0};
    // <o_custkey, o_orderkey, c_custkey>
    std::shared_ptr<PhysicalHashJoin> join = std::make_shared<PhysicalHashJoin>(JoinTypes::LEFT, build_ids, prode_ids);

    //  ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p2_custkey_col = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 2, 0);
    std::shared_ptr<BoundColumnRefExpression> p2_orderkey_col = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 1, 0);
    std::vector<std::shared_ptr<Expression>> expressions2 = {p2_custkey_col, p2_orderkey_col};
    // <c_custkey, o_orderkey>
    std::shared_ptr<PhysicalProject> project2 = std::make_shared<PhysicalProject>(expressions2);

    //  ================================================构建HashAgg================================================
    std::vector<int> group_set = {0};
    std::vector<int> agg_set = {1};
    std::vector<int> star_bitmap = {0};
    std::vector<AggFunctionType> aggregate_function_types = {AggFunctionType::COUNT};
    // <c_custkey, count(o_orderkey) AS c_count>
    std::shared_ptr<PhysicalMultiFieldHashAgg> hashAgg1 = std::make_shared<PhysicalMultiFieldHashAgg>(group_set, agg_set, star_bitmap, aggregate_function_types);

    //  ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p3_c_count = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 1, 0);
    std::vector<std::shared_ptr<Expression>> expressions3 = {p3_c_count};
    // <c_count>
    std::shared_ptr<PhysicalProject> project3 = std::make_shared<PhysicalProject>(expressions3);

    std::vector<int> group_set2 = {0};
    std::vector<int> agg_set2 = {0};
    std::vector<int> star_bitmap2 = {1};
    std::vector<AggFunctionType> aggregate_function_types2 = {AggFunctionType::COUNT};
    // <c_count, custdist>
    std::shared_ptr<PhysicalMultiFieldHashAgg> hashAgg2 = std::make_shared<PhysicalMultiFieldHashAgg>(group_set2, agg_set2, star_bitmap2, aggregate_function_types2);
    //  ================================================构建OrderBy================================================
    std::vector<int> sort_keys = {1, 0};
    std::vector<LogicalType> key_types = {LogicalType::INTEGER, LogicalType::INTEGER};
    std::vector<SortOrder> orders = {SortOrder::DESCENDING, SortOrder::DESCENDING};
    std::shared_ptr<PhysicalOrder> orderby = std::make_shared<PhysicalOrder>(&sort_keys, &key_types, &orders);

    orderby->AddChild(hashAgg2);
    hashAgg2->AddChild(project3);
    project3->AddChild(hashAgg1);
    hashAgg1->AddChild(project2);
    project2->AddChild(join);
    join->AddChild(project1);
    join->AddChild(scan_customer);
    project1->AddChild(filter);
    filter->AddChild(scan_orders);
    std::shared_ptr<Pipeline> pipeline = std::make_shared<Pipeline>();
    std::shared_ptr<PipelineGroup> pipeline_group = std::make_shared<PipelineGroup>();
    orderby->build_pipelines(pipeline, pipeline_group);
    std::shared_ptr<PipelineGroupExecute> pipeline_group_executor = std::make_shared<PipelineGroupExecute>(scheduler, pipeline_group);
    pipeline_group_executor->traverse_plan();
    auto start_execute = std::chrono::steady_clock::now();
    pipeline_group_executor->execute();
    auto end_execute = std::chrono::steady_clock::now();
    spdlog::info("[{} : {}] Q13执行时间: {}", __FILE__, __LINE__, Util::time_difference(start_execute, end_execute));

    // 关闭线程池
    scheduler->shutdown();
    RC expected = RC::SUCCESS;
    ASSERT_EQ(expected, RC::SUCCESS);
}

// 优化查询计划
TEST(TPCHTest, Q13FourThreadTest) {
    ////// step-1：构造表数据，分别往分区1和分区20插入数据
    std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
    bool importFinish = false;
    std::string fileDir = fileDir4;
    std::vector<int> work_ids(arr, arr + sizeof(arr) / sizeof(arr[0]));
    std::vector<int> partition_idxs(arr, arr + sizeof(arr) / sizeof(arr[0]));
    // 插入Customer表数据
    std::shared_ptr<Table> table_customer;
    std::vector<std::string> customer_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "customer.tbl_" + std::to_string(i);
        customer_file_names.emplace_back(file_name);
    }
    InsertCustomerMul(table_customer, scheduler, work_ids, partition_idxs, customer_file_names, importFinish);
    importFinish = false;
    // spdlog::info("[{} : {}] InsertCustomer Finish!!!", __FILE__, __LINE__);
    // 插入Orders表数据
    std::shared_ptr<Table> table_orders;
    std::vector<std::string> orders_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "orders.tbl_" + std::to_string(i);
        orders_file_names.emplace_back(file_name);
    }
    InsertOrdersMul(table_orders, scheduler, work_ids, partition_idxs, orders_file_names, importFinish);
    importFinish = false;
    // spdlog::info("[{} : {}] InsertOrders Finish!!!", __FILE__, __LINE__);

    std::cout << "Q13: SELECT c_count, count(*) AS custdist\n"
                 "FROM (\n"
                 "  SELECT c_custkey, count(o_orderkey) AS c_count\n"
                 "  FROM CUSTOMER\n"
                 "  LEFT OUTER JOIN ORDERS ON c_custkey = o_custkey\n"
                 "  AND o_comment NOT LIKE '%pending%deposits%'\n"
                 "  GROUP BY c_custkey\n"
                 ") c_orders\n"
                 "GROUP BY c_count\n"
                 "ORDER BY custdist DESC, c_count DESC;" << std::endl;

    //  ================================================构建Scan================================================
    std::vector<int> orders_ids = {0, 1, 8};
    // <o_orderkey, o_custkey, o_comment>
    std::shared_ptr<PhysicalTableScan> scan_orders = std::make_shared<PhysicalTableScan>(table_orders, -1, orders_ids);
    // <c_custkey>
    std::vector<int> customer_ids = {0};
    std::shared_ptr<PhysicalTableScan> scan_customer = std::make_shared<PhysicalTableScan>(table_customer,  -1, customer_ids);

    //  ================================================构建Filter================================================
    // 构建Filter表达式  o_comment NOT LIKE '%pending%deposits%'
    std::shared_ptr<BoundColumnRefExpression> left_comment = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 2, 0);
    std::string like_pattern = "%pending%deposits%";
    ScalarFunction bound_function = NotLikeFun::GetNotLikeFunction();
    Value value_pattern(like_pattern);
    std::shared_ptr<BoundConstantExpression> right_comment = std::make_shared<BoundConstantExpression>(value_pattern);
    std::vector<std::shared_ptr<Expression>> arguments;
    arguments.push_back(std::move(left_comment));
    arguments.push_back(std::move(right_comment));
    std::shared_ptr<BoundFunctionExpression> functionExpression = std::make_shared<BoundFunctionExpression>(LogicalType::BOOL, bound_function, arguments, nullptr);
    functionExpression->bind_info = functionExpression->function.bind(functionExpression->function, functionExpression->children);
    // <o_orderkey, o_custkey, o_comment>
    std::shared_ptr<PhysicalFilter> filter = std::make_shared<PhysicalFilter>(functionExpression);

    //  ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p1_custkey_col = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 1, 0);
    std::shared_ptr<BoundColumnRefExpression> p1_orderkey_col = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 0, 0);
    std::vector<std::shared_ptr<Expression>> expressions = {p1_custkey_col, p1_orderkey_col};
    // <o_custkey, o_orderkey>
    std::shared_ptr<PhysicalProject> project1 = std::make_shared<PhysicalProject>(expressions);

    //  ================================================构建HashJoin_LEFT================================================
    std::vector<int> build_ids = {0};
    std::vector<int> prode_ids = {0};
    // <o_custkey, o_orderkey, c_custkey>
    std::shared_ptr<PhysicalHashJoin> join = std::make_shared<PhysicalHashJoin>(JoinTypes::LEFT, build_ids, prode_ids);

    //  ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p2_custkey_col = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 2, 0);
    std::shared_ptr<BoundColumnRefExpression> p2_orderkey_col = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 1, 0);
    std::vector<std::shared_ptr<Expression>> expressions2 = {p2_custkey_col, p2_orderkey_col};
    // <c_custkey, o_orderkey>
    std::shared_ptr<PhysicalProject> project2 = std::make_shared<PhysicalProject>(expressions2);

    //  ================================================构建HashAgg================================================
    std::vector<int> group_set = {0};
    std::vector<int> agg_set = {1};
    std::vector<int> star_bitmap = {0};
    std::vector<AggFunctionType> aggregate_function_types = {AggFunctionType::COUNT};
    // <c_custkey, count(o_orderkey) AS c_count>
    std::shared_ptr<PhysicalMultiFieldHashAgg> hashAgg1 = std::make_shared<PhysicalMultiFieldHashAgg>(group_set, agg_set, star_bitmap, aggregate_function_types);

    //  ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p3_c_count = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 1, 0);
    std::vector<std::shared_ptr<Expression>> expressions3 = {p3_c_count};
    // <c_count>
    std::shared_ptr<PhysicalProject> project3 = std::make_shared<PhysicalProject>(expressions3);

    std::vector<int> group_set2 = {0};
    std::vector<int> agg_set2 = {0};
    std::vector<int> star_bitmap2 = {1};
    std::vector<AggFunctionType> aggregate_function_types2 = {AggFunctionType::COUNT};
    // <c_count, custdist>
    std::shared_ptr<PhysicalMultiFieldHashAgg> hashAgg2 = std::make_shared<PhysicalMultiFieldHashAgg>(group_set2, agg_set2, star_bitmap2, aggregate_function_types2);
    //  ================================================构建OrderBy================================================
    std::vector<int> sort_keys = {1, 0};
    std::vector<LogicalType> key_types = {LogicalType::DOUBLE, LogicalType::DOUBLE};
    std::vector<SortOrder> orders = {SortOrder::DESCENDING, SortOrder::DESCENDING};
    std::shared_ptr<PhysicalOrder> orderby = std::make_shared<PhysicalOrder>(&sort_keys, &key_types, &orders);

    orderby->AddChild(hashAgg2);
    hashAgg2->AddChild(project3);
    project3->AddChild(hashAgg1);
    hashAgg1->AddChild(project2);
    project2->AddChild(join);
    join->AddChild(project1);
    join->AddChild(scan_customer);
    project1->AddChild(filter);
    filter->AddChild(scan_orders);
    std::shared_ptr<Pipeline> pipeline = std::make_shared<Pipeline>();
    std::shared_ptr<PipelineGroup> pipeline_group = std::make_shared<PipelineGroup>();
    orderby->build_pipelines(pipeline, pipeline_group);
    std::shared_ptr<PipelineGroupExecute> pipeline_group_executor = std::make_shared<PipelineGroupExecute>(scheduler, pipeline_group);
    pipeline_group_executor->traverse_plan();
    pipeline_group_executor->execute();
    spdlog::info("[{} : {}] Q13执行时间: {} ms.", __FILE__, __LINE__, pipeline_group_executor->duration.count());
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