#include "import_data.hpp"
#include "file_dir_config.hpp"
// =============================Q4=============================
// 测试单线程Q4查询
TEST(TPCHTest, Q4SingleThreadTest) {
    std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
    bool importFinish = false;
    std::string fileDir = fileDir1;
    // 插入Orders表数据
    std::shared_ptr<Table> table_orders;
    InsertOrders(table_orders, scheduler, fileDir, importFinish);
    importFinish = false;

    // 插入Lineitem表数据
    std::shared_ptr<Table> table_lineitem;
    InsertLineitem(table_lineitem, scheduler, fileDir, importFinish);
    importFinish = false;

    std::cout << "Q4: select\n"
                 "    o_orderpriority,\n"
                 "    count(*) as order_count\n"
                 "from orders\n"
                 "where\n"
                 "    o_orderdate >= 741456000\n"
                 "    and o_orderdate < 749404800\n"
                 "    and exists (\n"
                 "        select\n"
                 "            *\n"
                 "        from\n"
                 "            lineitem\n"
                 "        where\n"
                 "            l_orderkey = o_orderkey\n"
                 "            and l_commitdate < l_receiptdate\n"
                 "    )\n"
                 "group by\n"
                 "    o_orderpriority\n"
                 "order by\n"
                 "    o_orderpriority;" << std::endl;

    //  ================================================构建Scan================================================
    std::vector<int> orders_ids = {0, 4, 5};
    // <o_orderkey, o_orderdate, o_orderpriority>
    std::shared_ptr<PhysicalTableScan> scan_orders = std::make_shared<PhysicalTableScan>(table_orders, -1, orders_ids);

    std::vector<int> lineitem_ids = {0, 11, 12};
    // <l_orderkey, l_commitdate, l_receiptdate>
    std::shared_ptr<PhysicalTableScan> scan_lineitem = std::make_shared<PhysicalTableScan>(table_lineitem, -1, lineitem_ids);

    //  ================================================构建Filter-orders================================================
    std::shared_ptr<BoundColumnRefExpression> left_o_orderdate = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 1, 0);
    Value value_741456000(741456000);
    std::shared_ptr<BoundConstantExpression> right_o_orderdate = std::make_shared<BoundConstantExpression>(value_741456000);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_o_orderdate = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_GREATERTHANOREQUALTO);
    comparisonExpression_o_orderdate->SetLeft(left_o_orderdate);
    comparisonExpression_o_orderdate->SetRight(right_o_orderdate);

    std::shared_ptr<BoundColumnRefExpression> left_o_orderdate2 = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 1, 0);
    Value value_749404800(749404800);
    std::shared_ptr<BoundConstantExpression> right_o_orderdate2 = std::make_shared<BoundConstantExpression>(value_749404800);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_o_orderdate2 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_LESSTHAN);
    comparisonExpression_o_orderdate2->SetLeft(left_o_orderdate2);
    comparisonExpression_o_orderdate2->SetRight(right_o_orderdate2);

    std::shared_ptr<BoundConjunctionExpression> conjunctionExpression_orders = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, comparisonExpression_o_orderdate, comparisonExpression_o_orderdate2);
    // <o_orderkey, o_orderdate, o_orderpriority>
    std::shared_ptr<PhysicalFilter> filter2 = std::make_shared<PhysicalFilter>(conjunctionExpression_orders);
    filter2->exp_name = "2";
    //  ================================================构建Filter-Lineitem================================================
    std::shared_ptr<BoundColumnRefExpression> left_l_commitdate = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 1, 0);
    std::shared_ptr<BoundColumnRefExpression> right_l_receiptdate = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 2, 0);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_l_commitdate = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_LESSTHAN);
    comparisonExpression_l_commitdate->SetLeft(left_l_commitdate);
    comparisonExpression_l_commitdate->SetRight(right_l_receiptdate);
    // <l_orderkey, l_commitdate, l_receiptdate>
    std::shared_ptr<PhysicalFilter> filter1 = std::make_shared<PhysicalFilter>(comparisonExpression_l_commitdate);

    //  ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p1_o_orderpriority = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 0, 0);
    std::vector<std::shared_ptr<Expression>> p1_expressions = {p1_o_orderpriority};
    // <l_orderkey>
    std::shared_ptr<PhysicalProject> project1 = std::make_shared<PhysicalProject>(p1_expressions);

    //  ================================================构建HashJoin ===================================================
    std::vector<int> build_ids1 = {0};
    std::vector<int> prode_ids1 = {0};
    // <o_orderkey, o_orderdate, o_orderpriority>
    std::shared_ptr<PhysicalHashJoin> join1 = std::make_shared<PhysicalHashJoin>(JoinTypes::SEMI, build_ids1, prode_ids1);

    //  ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p2_o_orderpriority = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 2, 0);
    std::vector<std::shared_ptr<Expression>> p2_expressions = {p2_o_orderpriority};
    // <o_orderpriority>
    std::shared_ptr<PhysicalProject> project2 = std::make_shared<PhysicalProject>(p2_expressions);

    //  ================================================构建HashAgg================================================
    std::vector<int> group_set = {0}; // GroupBy字段
    std::vector<int> agg_set = {0}; // 聚合字段
    std::vector<int> star_bitmap = {1}; // 对应字段是否为 ‘*’ 表
    std::vector<AggFunctionType> aggregate_function_types = { AggFunctionType::COUNT };
    // <o_orderpriority, Count(*)>
    std::shared_ptr<PhysicalMultiFieldHashAgg> hashAgg = std::make_shared<PhysicalMultiFieldHashAgg>(group_set, agg_set, star_bitmap, aggregate_function_types);

    //  ================================================构建OrderBy================================================
    std::vector<int> sort_keys = {0};
    std::vector<LogicalType> key_types = {LogicalType::STRING};
    std::vector<SortOrder> orders_p = {SortOrder::ASCENDING};
    std::shared_ptr<PhysicalOrder> orderby = std::make_shared<PhysicalOrder>(&sort_keys, &key_types, &orders_p);

    orderby->AddChild(hashAgg);
    hashAgg->AddChild(project2);
    project2->AddChild(join1);
    join1->AddChild(project1);
    join1->AddChild(filter2);
    project1->AddChild(filter1);
    filter1->AddChild(scan_lineitem);
    filter2->AddChild(scan_orders);

    std::shared_ptr<Pipeline> pipeline = std::make_shared<Pipeline>();
    std::shared_ptr<PipelineGroup> pipeline_group = std::make_shared<PipelineGroup>();
    orderby->build_pipelines(pipeline, pipeline_group);
    std::shared_ptr<PipelineGroupExecute> pipeline_group_executor = std::make_shared<PipelineGroupExecute>(scheduler, pipeline_group);
    pipeline_group_executor->traverse_plan();
    auto start_execute = std::chrono::steady_clock::now();
    pipeline_group_executor->execute();
    auto end_execute = std::chrono::steady_clock::now();
    spdlog::info("[{} : {}] Q4执行时间: {}", __FILE__, __LINE__, Util::time_difference(start_execute, end_execute));

    // 关闭线程池
    scheduler->shutdown();
    RC expected = RC::SUCCESS;
    ASSERT_EQ(expected, RC::SUCCESS);
}

// 测试4线程Q4查询
TEST(TPCHTest, Q4FourThreadTest) {
    std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
    bool importFinish = false;
    std::string fileDir = fileDir4;
    std::vector<int> work_ids(arr, arr + sizeof(arr) / sizeof(arr[0]));
    std::vector<int> partition_idxs(arr, arr + sizeof(arr) / sizeof(arr[0]));
    // 插入Orders表数据
    std::shared_ptr<Table> table_orders;
    std::vector<std::string> orders_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "orders.tbl_" + std::to_string(i);
        orders_file_names.emplace_back(file_name);
    }
    InsertOrdersMul(table_orders, scheduler, work_ids, partition_idxs, orders_file_names, importFinish);
    importFinish = false;

    // 插入Lineitem表数据
    std::shared_ptr<Table> table_lineitem;
    std::vector<std::string> lineitem_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "lineitem.tbl_" + std::to_string(i);
        lineitem_file_names.emplace_back(file_name);
    }
    InsertLineitemMul(table_lineitem, scheduler, work_ids, partition_idxs, lineitem_file_names, importFinish);
    importFinish = false;

    //  ================================================构建Scan================================================
    std::vector<int> orders_ids = {0, 4, 5};
    // <o_orderkey, o_orderdate, o_orderpriority>
    std::shared_ptr<PhysicalTableScan> scan_orders = std::make_shared<PhysicalTableScan>(table_orders, -1, orders_ids);

    std::vector<int> lineitem_ids = {0, 11, 12};
    // <l_orderkey, l_commitdate, l_receiptdate>
    std::shared_ptr<PhysicalTableScan> scan_lineitem = std::make_shared<PhysicalTableScan>(table_lineitem, -1, lineitem_ids);

    //  ================================================构建Filter-orders================================================
    std::shared_ptr<BoundColumnRefExpression> left_o_orderdate = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 1, 0);
    Value value_741456000(741456000);
    std::shared_ptr<BoundConstantExpression> right_o_orderdate = std::make_shared<BoundConstantExpression>(value_741456000);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_o_orderdate = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_GREATERTHANOREQUALTO);
    comparisonExpression_o_orderdate->SetLeft(left_o_orderdate);
    comparisonExpression_o_orderdate->SetRight(right_o_orderdate);

    std::shared_ptr<BoundColumnRefExpression> left_o_orderdate2 = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 1, 0);
    Value value_749404800(749404800);
    std::shared_ptr<BoundConstantExpression> right_o_orderdate2 = std::make_shared<BoundConstantExpression>(value_749404800);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_o_orderdate2 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_LESSTHAN);
    comparisonExpression_o_orderdate2->SetLeft(left_o_orderdate2);
    comparisonExpression_o_orderdate2->SetRight(right_o_orderdate2);

    std::shared_ptr<BoundConjunctionExpression> conjunctionExpression_orders = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, comparisonExpression_o_orderdate, comparisonExpression_o_orderdate2);
    // <o_orderkey, o_orderdate, o_orderpriority>
    std::shared_ptr<PhysicalFilter> filter2 = std::make_shared<PhysicalFilter>(conjunctionExpression_orders);
    filter2->exp_name = "2";
    //  ================================================构建Filter-Lineitem================================================
    std::shared_ptr<BoundColumnRefExpression> left_l_commitdate = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 1, 0);
    std::shared_ptr<BoundColumnRefExpression> right_l_receiptdate = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 2, 0);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_l_commitdate = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_LESSTHAN);
    comparisonExpression_l_commitdate->SetLeft(left_l_commitdate);
    comparisonExpression_l_commitdate->SetRight(right_l_receiptdate);
    // <l_orderkey, l_commitdate, l_receiptdate>
    std::shared_ptr<PhysicalFilter> filter1 = std::make_shared<PhysicalFilter>(comparisonExpression_l_commitdate);

    //  ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p1_o_orderpriority = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 0, 0);
    std::vector<std::shared_ptr<Expression>> p1_expressions = {p1_o_orderpriority};
    // <l_orderkey>
    std::shared_ptr<PhysicalProject> project1 = std::make_shared<PhysicalProject>(p1_expressions);

    //  ================================================构建HashJoin ===================================================
    std::vector<int> build_ids1 = {0};
    std::vector<int> prode_ids1 = {0};
    // <o_orderkey, o_orderdate, o_orderpriority>
    std::shared_ptr<PhysicalHashJoin> join1 = std::make_shared<PhysicalHashJoin>(JoinTypes::SEMI, build_ids1, prode_ids1);

    //  ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p2_o_orderpriority = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 2, 0);
    std::vector<std::shared_ptr<Expression>> p2_expressions = {p2_o_orderpriority};
    // <o_orderpriority>
    std::shared_ptr<PhysicalProject> project2 = std::make_shared<PhysicalProject>(p2_expressions);

    //  ================================================构建HashAgg================================================
    std::vector<int> group_set = {0}; // GroupBy字段
    std::vector<int> agg_set = {0}; // 聚合字段
    std::vector<int> star_bitmap = {1}; // 对应字段是否为 ‘*’ 表
    std::vector<AggFunctionType> aggregate_function_types = { AggFunctionType::COUNT };
    // <o_orderpriority, Count(*)>
    std::shared_ptr<PhysicalMultiFieldHashAgg> hashAgg = std::make_shared<PhysicalMultiFieldHashAgg>(group_set, agg_set, star_bitmap, aggregate_function_types);

    //  ================================================构建OrderBy================================================
    std::vector<int> sort_keys = {0};
    std::vector<LogicalType> key_types = {LogicalType::STRING};
    std::vector<SortOrder> orders_p = {SortOrder::ASCENDING};
    std::shared_ptr<PhysicalOrder> orderby = std::make_shared<PhysicalOrder>(&sort_keys, &key_types, &orders_p);

    orderby->AddChild(hashAgg);
    hashAgg->AddChild(project2);
    project2->AddChild(join1);
    join1->AddChild(project1);
    join1->AddChild(filter2);
    project1->AddChild(filter1);
    filter1->AddChild(scan_lineitem);
    filter2->AddChild(scan_orders);

    std::shared_ptr<Pipeline> pipeline = std::make_shared<Pipeline>();
    std::shared_ptr<PipelineGroup> pipeline_group = std::make_shared<PipelineGroup>();
    orderby->build_pipelines(pipeline, pipeline_group);
    std::shared_ptr<PipelineGroupExecute> pipeline_group_executor = std::make_shared<PipelineGroupExecute>(scheduler, pipeline_group);
    pipeline_group_executor->traverse_plan();
    pipeline_group_executor->execute();
    spdlog::info("[{} : {}] Q4执行时间: {} ms.", __FILE__, __LINE__, pipeline_group_executor->duration.count());
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