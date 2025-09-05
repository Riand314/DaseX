#include "import_data.hpp"
#include "file_dir_config.hpp"
// =============================Q18=============================
// 测试单线程Q18查询
TEST(TPCHTest, Q18SingleThreadTest) {
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
    importFinish = false;
    spdlog::info("[{} : {}] InsertOrders Finish!!!", __FILE__, __LINE__);

    // 插入Lineitem表数据
    std::shared_ptr<Table> table_lineitem;
    InsertLineitem(table_lineitem, scheduler, fileDir, importFinish);
    importFinish = false;
    spdlog::info("[{} : {}] InsertLineitem Finish!!!", __FILE__, __LINE__);

    //  ================================================构建Scan================================================
    std::vector<int> lineitem_ids1 = {0, 4};
    // <l_orderkey, l_quantity>
    std::shared_ptr<PhysicalTableScan> scan_lineitem1 = std::make_shared<PhysicalTableScan>(table_lineitem, -1, lineitem_ids1);

    std::vector<int> lineitem_ids2 = {0, 4};
    // <l_orderkey, l_quantity>
    std::shared_ptr<PhysicalTableScan> scan_lineitem2 = std::make_shared<PhysicalTableScan>(table_lineitem, -1, lineitem_ids2);

    std::vector<int> customer_ids = {0, 1};
    // <c_custkey, c_name>
    std::shared_ptr<PhysicalTableScan> scan_customer = std::make_shared<PhysicalTableScan>(table_customer, -1, customer_ids);

    std::vector<int> orders_ids = {0, 1, 3, 4};
    // <o_orderkey, o_custkey, o_totalprice, o_orderdate>
    std::shared_ptr<PhysicalTableScan> scan_orders = std::make_shared<PhysicalTableScan>(table_orders, -1, orders_ids);

    // ================================================构建HashAgg================================================
    std::vector<int> group_set = {0};
    std::vector<int> agg_set = {1};
    std::vector<int> star_bitmap = {0};
    std::vector<AggFunctionType> aggregate_function_types = {AggFunctionType::SUM};
    // <l_orderkey, Sum(l_quantity)>
    std::shared_ptr<PhysicalMultiFieldHashAgg> hashAgg1 = std::make_shared<PhysicalMultiFieldHashAgg>(group_set, agg_set, star_bitmap, aggregate_function_types);

    //  ================================================构建Filter================================================
    std::shared_ptr<BoundColumnRefExpression> f1_sum_l_quantity = std::make_shared<BoundColumnRefExpression>(LogicalType::DOUBLE, 0, 1, 0);

    Value value_300(300.0);
    std::shared_ptr<BoundConstantExpression> right_sum_l_quantity = std::make_shared<BoundConstantExpression>(value_300);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_GREATERTHAN, f1_sum_l_quantity, right_sum_l_quantity);
    // <l_orderkey, Sum(l_quantity)>
    std::shared_ptr<PhysicalFilter> filter1 = std::make_shared<PhysicalFilter>(comparisonExpression);
    filter1->exp_name = "1";

    //  ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p1_l_orderkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 0, 0);
    std::vector<std::shared_ptr<Expression>> p1_expressions = {p1_l_orderkey};
    // <l_orderkey>
    std::shared_ptr<PhysicalProject> project1 = std::make_shared<PhysicalProject>(p1_expressions);
    project1->exp_name = "1";

    //  ================================================构建Filter================================================
    std::shared_ptr<BoundColumnRefExpression> f2_c_custkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 0, 0);
    Value value_14999(customer_value);
    std::shared_ptr<BoundConstantExpression> right_c_custkey = std::make_shared<BoundConstantExpression>(value_14999);
    std::shared_ptr<BoundComparisonExpression> f2_comparisonExpression = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_LESSTHANOREQUALTO, f2_c_custkey, right_c_custkey);
    // <c_custkey, c_name>
    std::shared_ptr<PhysicalFilter> filter2 = std::make_shared<PhysicalFilter>(f2_comparisonExpression);
    filter2->exp_name = "2";

    //  ================================================构建HashJoin================================================
    std::vector<int> build_ids3 = {0};
    std::vector<int> prode_ids3 = {1};
    // <c_custkey, c_name, o_orderkey, o_custkey, o_totalprice, o_orderdate>
    std::shared_ptr<PhysicalHashJoin> join3 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids3, prode_ids3);
    join3->exp_name = "3";

    std::vector<int> build_ids2 = {2};
    std::vector<int> prode_ids2 = {0};
    // <c_custkey, c_name, o_orderkey, o_custkey, o_totalprice, o_orderdate, l_orderkey, l_quantity>
    std::shared_ptr<PhysicalHashJoin> join2 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids2, prode_ids2);
    join2->exp_name = "2";

    std::vector<int> build_ids1 = {0};
    std::vector<int> prode_ids1 = {2};
    // <c_custkey, c_name, o_orderkey, o_custkey, o_totalprice, o_orderdate, l_orderkey, l_quantity>
    std::shared_ptr<PhysicalHashJoin> join1 = std::make_shared<PhysicalHashJoin>(JoinTypes::SEMI, build_ids1, prode_ids1);
    join1->exp_name = "1";

    //  ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p2_l_quantity = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 7, 0);
    std::shared_ptr<BoundColumnRefExpression> p2_o_orderkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 2, 0);
    std::shared_ptr<BoundColumnRefExpression> p2_o_totalprice = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 4, 0);
    std::shared_ptr<BoundColumnRefExpression> p2_o_orderdate = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 5, 0);
    std::shared_ptr<BoundColumnRefExpression> p2_c_custkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 0, 0);
    std::shared_ptr<BoundColumnRefExpression> p2_c_name = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    std::vector<std::shared_ptr<Expression>> p2_expressions = {p2_l_quantity, p2_o_orderkey, p2_o_totalprice, p2_o_orderdate, p2_c_custkey, p2_c_name};
    // <l_quantity, o_orderkey, o_totalprice, o_orderdate, c_custkey, c_name>
    std::shared_ptr<PhysicalProject> project2 = std::make_shared<PhysicalProject>(p2_expressions);
    project2->exp_name = "2";

    // 构建HashAgg
    std::vector<int> group_set2 = {5, 4, 1, 3, 2};
    std::vector<int> agg_set2 = {0};
    std::vector<int> star_bitmap2 = {0};
    std::vector<AggFunctionType> aggregate_function_types2 = {AggFunctionType::SUM};
    // <c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, Sum(l_quantity)>
    std::shared_ptr<PhysicalMultiFieldHashAgg> hashAgg2 = std::make_shared<PhysicalMultiFieldHashAgg>(group_set2, agg_set2, star_bitmap2, aggregate_function_types2);

    // 构建OrderBy
    std::vector<int> sort_keys = {4, 3};
    std::vector<LogicalType> key_types = {LogicalType::FLOAT, LogicalType::INTEGER};
    std::vector<SortOrder> orders_p = {SortOrder::DESCENDING, SortOrder::ASCENDING};
    std::shared_ptr<PhysicalOrder> orderby = std::make_shared<PhysicalOrder>(&sort_keys, &key_types, &orders_p);

    orderby->AddChild(hashAgg2);
    hashAgg2->AddChild(project2);
    project2->AddChild(join1);
    join1->AddChild(project1);
    join1->AddChild(join2);
    project1->AddChild(filter1);
    filter1->AddChild(hashAgg1);
    hashAgg1->AddChild(scan_lineitem1);
    join2->AddChild(join3);
    join2->AddChild(scan_lineitem2);
    join3->AddChild(filter2);
    join3->AddChild(scan_orders);
    filter2->AddChild(scan_customer);

    std::shared_ptr<Pipeline> pipeline = std::make_shared<Pipeline>();
    std::shared_ptr<PipelineGroup> pipeline_group = std::make_shared<PipelineGroup>();
    orderby->build_pipelines(pipeline, pipeline_group);
    std::shared_ptr<PipelineGroupExecute> pipeline_group_executor = std::make_shared<PipelineGroupExecute>(scheduler, pipeline_group);
    pipeline_group_executor->traverse_plan();
    auto start_execute = std::chrono::steady_clock::now();
    pipeline_group_executor->execute();
    auto end_execute = std::chrono::steady_clock::now();
    spdlog::info("[{} : {}] Q18执行时间: {}", __FILE__, __LINE__, Util::time_difference(start_execute, end_execute));
    // 关闭线程池
    scheduler->shutdown();
    RC expected = RC::SUCCESS;
    ASSERT_EQ(expected, RC::SUCCESS);
}

// 测试4线程Q18查询
TEST(TPCHTest, Q18FourThreadTest) {
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
    std::this_thread::sleep_for(std::chrono::seconds(2));
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
    std::this_thread::sleep_for(std::chrono::seconds(2));
    // spdlog::info("[{} : {}] InsertOrders Finish!!!", __FILE__, __LINE__);

    // 插入Lineitem表数据
    std::shared_ptr<Table> table_lineitem;
    std::vector<std::string> lineitem_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "lineitem.tbl_" + std::to_string(i);
        lineitem_file_names.emplace_back(file_name);
    }
    InsertLineitemMul(table_lineitem, scheduler, work_ids, partition_idxs, lineitem_file_names, importFinish);
    importFinish = false;
    std::this_thread::sleep_for(std::chrono::seconds(2));
    // spdlog::info("[{} : {}] InsertLineitem Finish!!!", __FILE__, __LINE__);

    std::cout << "Q18: select\n"
                 "    c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice,\n"
                 "    sum(l_quantity)\n"
                 "from\n"
                 "    customer, orders, lineitem\n"
                 "where\n"
                 "    o_orderkey in (\n"
                 "        select\n"
                 "            l_orderkey\n"
                 "        from\n"
                 "            lineitem\n"
                 "        group by \n"
                 "            l_orderkey \n"
                 "    having\n"
                 "            sum(l_quantity) > 300\n"
                 "    )\n"
                 "    and c_custkey = o_custkey\n"
                 "    and o_orderkey = l_orderkey \n"
                 "group by\n"
                 "    c_name,\n"
                 "    c_custkey,\n"
                 "    o_orderkey,\n"
                 "    o_orderdate,\n"
                 "    o_totalprice\n"
                 "order by\n"
                 "    o_totalprice desc,\n"
                 "    o_orderdate;" << std::endl;

    //  ================================================构建Scan================================================
    std::vector<int> lineitem_ids1 = {0, 4};
    // <l_orderkey, l_quantity>
    std::shared_ptr<PhysicalTableScan> scan_lineitem1 = std::make_shared<PhysicalTableScan>(table_lineitem, -1, lineitem_ids1);

    std::vector<int> lineitem_ids2 = {0, 4};
    // <l_orderkey, l_quantity>
    std::shared_ptr<PhysicalTableScan> scan_lineitem2 = std::make_shared<PhysicalTableScan>(table_lineitem, -1, lineitem_ids2);

    std::vector<int> customer_ids = {0, 1};
    // <c_custkey, c_name>
    std::shared_ptr<PhysicalTableScan> scan_customer = std::make_shared<PhysicalTableScan>(table_customer, -1, customer_ids);

    std::vector<int> orders_ids = {0, 1, 3, 4};
    // <o_orderkey, o_custkey, o_totalprice, o_orderdate>
    std::shared_ptr<PhysicalTableScan> scan_orders = std::make_shared<PhysicalTableScan>(table_orders, -1, orders_ids);

    // ================================================构建HashAgg================================================
    std::vector<int> group_set = {0};
    std::vector<int> agg_set = {1};
    std::vector<int> star_bitmap = {0};
    std::vector<AggFunctionType> aggregate_function_types = {AggFunctionType::SUM};
    // <l_orderkey, Sum(l_quantity)>
    std::shared_ptr<PhysicalMultiFieldHashAgg> hashAgg1 = std::make_shared<PhysicalMultiFieldHashAgg>(group_set, agg_set, star_bitmap, aggregate_function_types);
    hashAgg1->exp_name = "1";
    //  ================================================构建Filter================================================
    std::shared_ptr<BoundColumnRefExpression> f1_sum_l_quantity = std::make_shared<BoundColumnRefExpression>(LogicalType::DOUBLE, 0, 1, 0);

    Value value_300(300.0);
    std::shared_ptr<BoundConstantExpression> right_sum_l_quantity = std::make_shared<BoundConstantExpression>(value_300);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_GREATERTHAN, f1_sum_l_quantity, right_sum_l_quantity);
    // <l_orderkey, Sum(l_quantity)>
    std::shared_ptr<PhysicalFilter> filter1 = std::make_shared<PhysicalFilter>(comparisonExpression);
    filter1->exp_name = "1";

    //  ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p1_l_orderkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 0, 0);
    std::vector<std::shared_ptr<Expression>> p1_expressions = {p1_l_orderkey};
    // <l_orderkey>
    std::shared_ptr<PhysicalProject> project1 = std::make_shared<PhysicalProject>(p1_expressions);
    project1->exp_name = "1";

    //  ================================================构建Filter================================================
    std::shared_ptr<BoundColumnRefExpression> f2_c_custkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 0, 0);
    Value value_14999(customer_value);
    std::shared_ptr<BoundConstantExpression> right_c_custkey = std::make_shared<BoundConstantExpression>(value_14999);
    std::shared_ptr<BoundComparisonExpression> f2_comparisonExpression = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_LESSTHANOREQUALTO, f2_c_custkey, right_c_custkey);
    // <c_custkey, c_name>
    std::shared_ptr<PhysicalFilter> filter2 = std::make_shared<PhysicalFilter>(f2_comparisonExpression);
    filter2->exp_name = "2";

    //  ================================================构建HashJoin================================================
    std::vector<int> build_ids3 = {0};
    std::vector<int> prode_ids3 = {1};
    // <c_custkey, c_name, o_orderkey, o_custkey, o_totalprice, o_orderdate>
    std::shared_ptr<PhysicalHashJoin> join3 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids3, prode_ids3);
    join3->exp_name = "3";

    std::vector<int> build_ids2 = {2};
    std::vector<int> prode_ids2 = {0};
    // <c_custkey, c_name, o_orderkey, o_custkey, o_totalprice, o_orderdate, l_orderkey, l_quantity>
    std::shared_ptr<PhysicalHashJoin> join2 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids2, prode_ids2);
    join2->exp_name = "2";

    std::vector<int> build_ids1 = {0};
    std::vector<int> prode_ids1 = {2};
    // <c_custkey, c_name, o_orderkey, o_custkey, o_totalprice, o_orderdate, l_orderkey, l_quantity>
    std::shared_ptr<PhysicalHashJoin> join1 = std::make_shared<PhysicalHashJoin>(JoinTypes::SEMI, build_ids1, prode_ids1);
    join1->exp_name = "1";

    //  ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p2_l_quantity = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 7, 0);
    std::shared_ptr<BoundColumnRefExpression> p2_o_orderkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 2, 0);
    std::shared_ptr<BoundColumnRefExpression> p2_o_totalprice = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 4, 0);
    std::shared_ptr<BoundColumnRefExpression> p2_o_orderdate = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 5, 0);
    std::shared_ptr<BoundColumnRefExpression> p2_c_custkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 0, 0);
    std::shared_ptr<BoundColumnRefExpression> p2_c_name = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    std::vector<std::shared_ptr<Expression>> p2_expressions = {p2_l_quantity, p2_o_orderkey, p2_o_totalprice, p2_o_orderdate, p2_c_custkey, p2_c_name};
    // <l_quantity, o_orderkey, o_totalprice, o_orderdate, c_custkey, c_name>
    std::shared_ptr<PhysicalProject> project2 = std::make_shared<PhysicalProject>(p2_expressions);
    project2->exp_name = "2";

    // 构建HashAgg
    std::vector<int> group_set2 = {5, 4, 1, 3, 2};
    std::vector<int> agg_set2 = {0};
    std::vector<int> star_bitmap2 = {0};
    std::vector<AggFunctionType> aggregate_function_types2 = {AggFunctionType::SUM};
    // <c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, Sum(l_quantity)>
    std::shared_ptr<PhysicalMultiFieldHashAgg> hashAgg2 = std::make_shared<PhysicalMultiFieldHashAgg>(group_set2, agg_set2, star_bitmap2, aggregate_function_types2);

    // 构建OrderBy
    std::vector<int> sort_keys = {4, 3};
    std::vector<LogicalType> key_types = {LogicalType::FLOAT, LogicalType::INTEGER};
    std::vector<SortOrder> orders_p = {SortOrder::DESCENDING, SortOrder::ASCENDING};
    std::shared_ptr<PhysicalOrder> orderby = std::make_shared<PhysicalOrder>(&sort_keys, &key_types, &orders_p);

    orderby->AddChild(hashAgg2);
    hashAgg2->AddChild(project2);
    project2->AddChild(join1);
    join1->AddChild(project1);
    join1->AddChild(join2);
    project1->AddChild(filter1);
    filter1->AddChild(hashAgg1);
    hashAgg1->AddChild(scan_lineitem1);
    join2->AddChild(join3);
    join2->AddChild(scan_lineitem2);
    join3->AddChild(filter2);
    join3->AddChild(scan_orders);
    filter2->AddChild(scan_customer);

    std::shared_ptr<Pipeline> pipeline = std::make_shared<Pipeline>();
    std::shared_ptr<PipelineGroup> pipeline_group = std::make_shared<PipelineGroup>();
    orderby->build_pipelines(pipeline, pipeline_group);
    std::shared_ptr<PipelineGroupExecute> pipeline_group_executor = std::make_shared<PipelineGroupExecute>(scheduler, pipeline_group);
    pipeline_group_executor->traverse_plan();
    pipeline_group_executor->execute();
    spdlog::info("[{} : {}] Q18执行时间: {} ms.", __FILE__, __LINE__, pipeline_group_executor->duration.count());
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