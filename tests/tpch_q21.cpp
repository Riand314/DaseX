#include "import_data.hpp"
#include "file_dir_config.hpp"
// =============================Q21=============================
// 测试单线程Q21查询
TEST(TPCHTest, Q21SingleThreadTest) {
    std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
    bool importFinish = false;
    std::string fileDir = fileDir1;
    // 插入Lineitem表数据
    std::shared_ptr<Table> table_lineitem;
    InsertLineitem(table_lineitem, scheduler, fileDir, importFinish);
    importFinish = false;
    spdlog::info("[{} : {}] InsertLineitem Finish!!!", __FILE__, __LINE__);

    // 插入Nation表数据
    std::shared_ptr<Table> table_nation;
    InsertNation(table_nation, scheduler, fileDir, importFinish);
    importFinish = false;
    spdlog::info("[{} : {}] InsertNation Finish!!!", __FILE__, __LINE__);

    // 插入Supplier表数据
    std::shared_ptr<Table> table_supplier;
    InsertSupplier(table_supplier, scheduler, fileDir, importFinish);
    importFinish = false;
    spdlog::info("[{} : {}] InsertSupplier Finish!!!", __FILE__, __LINE__);

    // 插入Orders表数据
    std::shared_ptr<Table> table_orders;
    InsertOrders(table_orders, scheduler, fileDir, importFinish);
    importFinish = false;
    spdlog::info("[{} : {}] InsertOrders Finish!!!", __FILE__, __LINE__);

    //  ================================================构建Scan================================================
    std::vector<int> lineitem_ids1 = {0,2,11,12};
    // <l_orderkey, l_suppkey, l_commitdate, l_receiptdate>
    std::shared_ptr<PhysicalTableScan> scan_lineitem1 = std::make_shared<PhysicalTableScan>(table_lineitem, -1, lineitem_ids1);

    std::vector<int> lineitem_ids2 = {0,2};
    // <l_orderkey, l_suppkey>
    std::shared_ptr<PhysicalTableScan> scan_lineitem2 = std::make_shared<PhysicalTableScan>(table_lineitem, -1, lineitem_ids2);

    std::vector<int> lineitem_ids3 = {0,2,11,12};
    // <l_orderkey, l_suppkey, l_commitdate, l_receiptdate>
    std::shared_ptr<PhysicalTableScan> scan_lineitem3 = std::make_shared<PhysicalTableScan>(table_lineitem, -1, lineitem_ids3);

    std::vector<int> nation_ids = {0,1};
    // <n_nationkey, n_name>
    std::shared_ptr<PhysicalTableScan> scan_nation = std::make_shared<PhysicalTableScan>(table_nation, -1, nation_ids);

    std::vector<int> supplier_ids = {0,1,3};
    // <s_suppkey, s_name, s_nationkey>
    std::shared_ptr<PhysicalTableScan> scan_supplier = std::make_shared<PhysicalTableScan>(table_supplier, -1, supplier_ids);

    std::vector<int> orders_ids = {0,2};
    // <o_orderkey, o_orderstatus>
    std::shared_ptr<PhysicalTableScan> scan_orders = std::make_shared<PhysicalTableScan>(table_orders, -1, orders_ids);

    //  ================================================构建Filter================================================
    std::shared_ptr<BoundColumnRefExpression> f1_n_name = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    Value value_pattern("SAUDI ARABIA");
    std::shared_ptr<BoundConstantExpression> right_n_name = std::make_shared<BoundConstantExpression>(value_pattern);
    std::shared_ptr<BoundComparisonExpression> f1_comparisonExpression = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_EQUAL, f1_n_name, right_n_name);
    // <n_nationkey, n_name>
    std::shared_ptr<PhysicalFilter> filter1 = std::make_shared<PhysicalFilter>(f1_comparisonExpression);
    filter1->exp_name = "1";

    std::shared_ptr<BoundColumnRefExpression> f2_l_receiptdate = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 3, 0);
    std::shared_ptr<BoundColumnRefExpression> f2_l_commitdate = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 2, 0);
    std::shared_ptr<BoundComparisonExpression> f2_comparisonExpression = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_GREATERTHAN, f2_l_receiptdate, f2_l_commitdate);
    // <l_orderkey, l_suppkey, l_commitdate, l_receiptdate>
    std::shared_ptr<PhysicalFilter> filter2 = std::make_shared<PhysicalFilter>(f2_comparisonExpression);
    filter2->exp_name = "2";

    std::shared_ptr<BoundColumnRefExpression> f3_o_orderstatus = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    Value value_F("F");
    std::shared_ptr<BoundConstantExpression> right_o_orderstatus = std::make_shared<BoundConstantExpression>(value_F);
    std::shared_ptr<BoundComparisonExpression> f3_comparisonExpression = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_EQUAL, f3_o_orderstatus, right_o_orderstatus);
    // <o_orderkey, o_orderstatus>
    std::shared_ptr<PhysicalFilter> filter3 = std::make_shared<PhysicalFilter>(f3_comparisonExpression);
    filter3->exp_name = "3";

    //  ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p1_l_orderkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 0, 0);
    std::shared_ptr<BoundColumnRefExpression> p1_l_suppkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 1, 0);
    std::vector<std::shared_ptr<Expression>> p1_expressions = {p1_l_orderkey, p1_l_suppkey};
    // <l_orderkey, l_suppkey>
    std::shared_ptr<PhysicalProject> project1 = std::make_shared<PhysicalProject>(p1_expressions);
    project1->exp_name = "1";

    //  ================================================构建HashJoin================================================
    std::vector<int> build_ids = {0};
    std::vector<int> prode_ids = {2};
    // <n_nationkey, n_name, s_suppkey, s_name, s_nationkey>
    std::shared_ptr<PhysicalHashJoin> join1 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids, prode_ids);
    join1->exp_name = "1";

    std::vector<int> build_ids2 = {2};
    std::vector<int> prode_ids2 = {1};
    // <n_nationkey, n_name, s_suppkey, s_name, s_nationkey, l_orderkey, l_suppkey>
    std::shared_ptr<PhysicalHashJoin> join2 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids2, prode_ids2);
    join2->exp_name = "2";

    std::vector<int> build_ids3 = {5};
    std::vector<int> prode_ids3 = {0};
    // <n_nationkey, n_name, s_suppkey, s_name, s_nationkey, l_orderkey, l_suppkey, o_orderkey, o_orderstatus>
    std::shared_ptr<PhysicalHashJoin> join3 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids3, prode_ids3);
    join3->exp_name = "3";

    // TODO: 支持!=比较
    std::vector<int> build_ids4 = {0,1};
    std::vector<int> prode_ids4 = {5,6};
    std::vector<ExpressionTypes> types = {ExpressionTypes::COMPARE_EQUAL, ExpressionTypes::COMPARE_NOTEQUAL};
    // <n_nationkey, n_name, s_suppkey, s_name, s_nationkey, l_orderkey, l_suppkey, o_orderkey, o_orderstatus>
    std::shared_ptr<PhysicalHashJoin> join4 = std::make_shared<PhysicalHashJoin>(JoinTypes::SEMI, build_ids4, prode_ids4, types);
    join4->exp_name = "4";

    std::shared_ptr<BoundColumnRefExpression> f4_l_receiptdate = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 3, 0);
    std::shared_ptr<BoundColumnRefExpression> f4_l_commitdate = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 2, 0);
    std::shared_ptr<BoundComparisonExpression> f4_comparisonExpression = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_GREATERTHAN, f4_l_receiptdate, f4_l_commitdate);
    // <l_orderkey, l_suppkey, l_commitdate, l_receiptdate>
    std::shared_ptr<PhysicalFilter> filter4 = std::make_shared<PhysicalFilter>(f4_comparisonExpression);
    filter4->exp_name = "4";

    std::shared_ptr<BoundColumnRefExpression> p2_l_orderkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 0, 0);
    std::shared_ptr<BoundColumnRefExpression> p2_l_suppkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 1, 0);
    std::vector<std::shared_ptr<Expression>> p2_expressions = {p2_l_orderkey, p2_l_suppkey};
    // <l_orderkey, l_suppkey>
    std::shared_ptr<PhysicalProject> project2 = std::make_shared<PhysicalProject>(p2_expressions);
    project2->exp_name = "2";

    std::vector<int> build_ids5 = {0,1};
    std::vector<int> prode_ids5 = {5,6};
    // <n_nationkey, n_name, s_suppkey, s_name, s_nationkey, l_orderkey, l_suppkey, o_orderkey, o_orderstatus>
    std::shared_ptr<PhysicalHashJoin> join5 = std::make_shared<PhysicalHashJoin>(JoinTypes::ANTI, build_ids5, prode_ids5, types);
    join5->exp_name = "5";

    std::shared_ptr<BoundColumnRefExpression> p3_s_name = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 3, 0);
    std::vector<std::shared_ptr<Expression>> p3_expressions = {p3_s_name};
    // <s_name>
    std::shared_ptr<PhysicalProject> project3 = std::make_shared<PhysicalProject>(p3_expressions);
    project3->exp_name = "3";

    // ================================================构建HashAgg================================================
    // 构建HashAgg
    std::vector<int> group_set = {0};
    std::vector<int> agg_set = {0};
    std::vector<int> star_bitmap = {1};
    std::vector<AggFunctionType> aggregate_function_types = {AggFunctionType::COUNT};
    // <s_name, Count(*)>
    std::shared_ptr<PhysicalMultiFieldHashAgg> hashAgg1 = std::make_shared<PhysicalMultiFieldHashAgg>(group_set, agg_set, star_bitmap, aggregate_function_types);

    //  ================================================构建OrderBy================================================
    std::vector<int> sort_keys = {1, 0};
    std::vector<LogicalType> key_types = {LogicalType::INTEGER, LogicalType::STRING};
    std::vector<SortOrder> orders_p = {SortOrder::DESCENDING, SortOrder::ASCENDING};
    std::shared_ptr<PhysicalOrder> orderby = std::make_shared<PhysicalOrder>(&sort_keys, &key_types, &orders_p);

    orderby->AddChild(hashAgg1);
    hashAgg1->AddChild(project3);
    project3->AddChild(join5);
    join5->AddChild(project2);
    project2->AddChild(filter4);
    filter4->AddChild(scan_lineitem3);
    join5->AddChild(join4);
    join4->AddChild(scan_lineitem2);
    join4->AddChild(join3);
    join3->AddChild(join2);
    join3->AddChild(filter3);
    filter3->AddChild(scan_orders);
    join2->AddChild(join1);
    join2->AddChild(project1);
    project1->AddChild(filter2);
    filter2->AddChild(scan_lineitem1);
    join1->AddChild(filter1);
    join1->AddChild(scan_supplier);
    filter1->AddChild(scan_nation);

    std::shared_ptr<Pipeline> pipeline = std::make_shared<Pipeline>();
    std::shared_ptr<PipelineGroup> pipeline_group = std::make_shared<PipelineGroup>();
    orderby->build_pipelines(pipeline, pipeline_group);
    std::shared_ptr<PipelineGroupExecute> pipeline_group_executor = std::make_shared<PipelineGroupExecute>(scheduler, pipeline_group);
    pipeline_group_executor->traverse_plan();
    auto start_execute = std::chrono::steady_clock::now();
    pipeline_group_executor->execute();
    auto end_execute = std::chrono::steady_clock::now();
    spdlog::info("[{} : {}] Q21执行时间: {}", __FILE__, __LINE__, Util::time_difference(start_execute, end_execute));

    // 关闭线程池
    scheduler->shutdown();
    RC expected = RC::SUCCESS;
    ASSERT_EQ(expected, RC::SUCCESS);
}

// 测试4线程Q21查询
TEST(TPCHTest, Q21FourThreadTest) {
    std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
    bool importFinish = false;
    std::string fileDir = fileDir4;
    std::vector<int> work_ids(arr, arr + sizeof(arr) / sizeof(arr[0]));
    std::vector<int> partition_idxs(arr, arr + sizeof(arr) / sizeof(arr[0]));
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

    // 插入Nation表数据
    std::shared_ptr<Table> table_nation;
    std::vector<std::string> nation_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "nation.tbl_" + std::to_string(i);
        nation_file_names.emplace_back(file_name);
    }
    InsertNationMul(table_nation, scheduler, work_ids, partition_idxs, nation_file_names, importFinish);
    importFinish = false;
    // spdlog::info("[{} : {}] InsertNation Finish!!!", __FILE__, __LINE__);

    // 插入Supplier表数据
    std::shared_ptr<Table> table_supplier;
    std::vector<std::string> supplier_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "supplier.tbl_" + std::to_string(i);
        supplier_file_names.emplace_back(file_name);
    }
    InsertSupplierMul(table_supplier, scheduler, work_ids, partition_idxs, supplier_file_names, importFinish);
    importFinish = false;
    std::this_thread::sleep_for(std::chrono::seconds(2));
    // spdlog::info("[{} : {}] InsertSupplier Finish!!!", __FILE__, __LINE__);

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

    std::cout << "Q21: select\n"
                 "    s_name, count(*) as numwait\n"
                 "from\n"
                 "    supplier, lineitem l1, orders, nation\n"
                 "where\n"
                 "    s_suppkey = l1.l_suppkey\n"
                 "    and o_orderkey = l1.l_orderkey\n"
                 "    and o_orderstatus = 'F'\n"
                 "    and l1.l_receiptdate > l1.l_commitdate\n"
                 "    and exists (\n"
                 "        select\n"
                 "            *\n"
                 "        from\n"
                 "            lineitem l2\n"
                 "        where\n"
                 "            l2.l_orderkey = l1.l_orderkey\n"
                 "            and l2.l_suppkey <> l1.l_suppkey\n"
                 "    )\n"
                 "    and not exists (\n"
                 "        select\n"
                 "            *\n"
                 "        from\n"
                 "            lineitem l3\n"
                 "        where\n"
                 "            l3.l_orderkey = l1.l_orderkey\n"
                 "            and l3.l_suppkey <> l1.l_suppkey\n"
                 "            and l3.l_receiptdate > l3.l_commitdate\n"
                 "    )\n"
                 "    and s_nationkey = n_nationkey\n"
                 "    and n_name = 'SAUDI ARABIA'\n"
                 "group by\n"
                 "    s_name\n"
                 "order by\n"
                 "    numwait desc,\n"
                 "    s_name;" << std::endl;

    //  ================================================构建Scan================================================
    std::vector<int> lineitem_ids1 = {0,2,11,12};
    // <l_orderkey, l_suppkey, l_commitdate, l_receiptdate>
    std::shared_ptr<PhysicalTableScan> scan_lineitem1 = std::make_shared<PhysicalTableScan>(table_lineitem, -1, lineitem_ids1);

    std::vector<int> lineitem_ids2 = {0,2};
    // <l_orderkey, l_suppkey>
    std::shared_ptr<PhysicalTableScan> scan_lineitem2 = std::make_shared<PhysicalTableScan>(table_lineitem, -1, lineitem_ids2);

    std::vector<int> lineitem_ids3 = {0,2,11,12};
    // <l_orderkey, l_suppkey, l_commitdate, l_receiptdate>
    std::shared_ptr<PhysicalTableScan> scan_lineitem3 = std::make_shared<PhysicalTableScan>(table_lineitem, -1, lineitem_ids3);

    std::vector<int> nation_ids = {0,1};
    // <n_nationkey, n_name>
    std::shared_ptr<PhysicalTableScan> scan_nation = std::make_shared<PhysicalTableScan>(table_nation, -1, nation_ids);

    std::vector<int> supplier_ids = {0,1,3};
    // <s_suppkey, s_name, s_nationkey>
    std::shared_ptr<PhysicalTableScan> scan_supplier = std::make_shared<PhysicalTableScan>(table_supplier, -1, supplier_ids);

    std::vector<int> orders_ids = {0,2};
    // <o_orderkey, o_orderstatus>
    std::shared_ptr<PhysicalTableScan> scan_orders = std::make_shared<PhysicalTableScan>(table_orders, -1, orders_ids);

    //  ================================================构建Filter================================================
    std::shared_ptr<BoundColumnRefExpression> f1_n_name = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    Value value_pattern("SAUDI ARABIA");
    std::shared_ptr<BoundConstantExpression> right_n_name = std::make_shared<BoundConstantExpression>(value_pattern);
    std::shared_ptr<BoundComparisonExpression> f1_comparisonExpression = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_EQUAL, f1_n_name, right_n_name);
    // <n_nationkey, n_name>
    std::shared_ptr<PhysicalFilter> filter1 = std::make_shared<PhysicalFilter>(f1_comparisonExpression);
    filter1->exp_name = "1";

    std::shared_ptr<BoundColumnRefExpression> f2_l_receiptdate = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 3, 0);
    std::shared_ptr<BoundColumnRefExpression> f2_l_commitdate = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 2, 0);
    std::shared_ptr<BoundComparisonExpression> f2_comparisonExpression = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_GREATERTHAN, f2_l_receiptdate, f2_l_commitdate);
    // <l_orderkey, l_suppkey, l_commitdate, l_receiptdate>
    std::shared_ptr<PhysicalFilter> filter2 = std::make_shared<PhysicalFilter>(f2_comparisonExpression);
    filter2->exp_name = "2";

    std::shared_ptr<BoundColumnRefExpression> f3_o_orderstatus = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    Value value_F("F");
    std::shared_ptr<BoundConstantExpression> right_o_orderstatus = std::make_shared<BoundConstantExpression>(value_F);
    std::shared_ptr<BoundComparisonExpression> f3_comparisonExpression = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_EQUAL, f3_o_orderstatus, right_o_orderstatus);
    // <o_orderkey, o_orderstatus>
    std::shared_ptr<PhysicalFilter> filter3 = std::make_shared<PhysicalFilter>(f3_comparisonExpression);
    filter3->exp_name = "3";

    //  ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p1_l_orderkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 0, 0);
    std::shared_ptr<BoundColumnRefExpression> p1_l_suppkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 1, 0);
    std::vector<std::shared_ptr<Expression>> p1_expressions = {p1_l_orderkey, p1_l_suppkey};
    // <l_orderkey, l_suppkey>
    std::shared_ptr<PhysicalProject> project1 = std::make_shared<PhysicalProject>(p1_expressions);
    project1->exp_name = "1";

    //  ================================================构建HashJoin================================================
    std::vector<int> build_ids = {0};
    std::vector<int> prode_ids = {2};
    std::vector<ExpressionTypes> comparison_types1 = {};
    // <n_nationkey, n_name, s_suppkey, s_name, s_nationkey>
    std::shared_ptr<PhysicalHashJoin> join1 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids, prode_ids, comparison_types1, true);
    join1->exp_name = "1";

    std::vector<int> build_ids2 = {2};
    std::vector<int> prode_ids2 = {1};
    // <n_nationkey, n_name, s_suppkey, s_name, s_nationkey, l_orderkey, l_suppkey>
    std::shared_ptr<PhysicalHashJoin> join2 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids2, prode_ids2);
    join2->exp_name = "2";

    std::vector<int> build_ids3 = {5};
    std::vector<int> prode_ids3 = {0};
    // <n_nationkey, n_name, s_suppkey, s_name, s_nationkey, l_orderkey, l_suppkey, o_orderkey, o_orderstatus>
    std::shared_ptr<PhysicalHashJoin> join3 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids3, prode_ids3);
    join3->exp_name = "3";

    // TODO: 支持!=比较
    std::vector<int> build_ids4 = {0,1};
    std::vector<int> prode_ids4 = {5,6};
    std::vector<ExpressionTypes> types = {ExpressionTypes::COMPARE_EQUAL, ExpressionTypes::COMPARE_NOTEQUAL};
    // <n_nationkey, n_name, s_suppkey, s_name, s_nationkey, l_orderkey, l_suppkey, o_orderkey, o_orderstatus>
    std::shared_ptr<PhysicalHashJoin> join4 = std::make_shared<PhysicalHashJoin>(JoinTypes::SEMI, build_ids4, prode_ids4, types);
    join4->exp_name = "4";

    std::shared_ptr<BoundColumnRefExpression> f4_l_receiptdate = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 3, 0);
    std::shared_ptr<BoundColumnRefExpression> f4_l_commitdate = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 2, 0);
    std::shared_ptr<BoundComparisonExpression> f4_comparisonExpression = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_GREATERTHAN, f4_l_receiptdate, f4_l_commitdate);
    // <l_orderkey, l_suppkey, l_commitdate, l_receiptdate>
    std::shared_ptr<PhysicalFilter> filter4 = std::make_shared<PhysicalFilter>(f4_comparisonExpression);
    filter4->exp_name = "4";

    std::shared_ptr<BoundColumnRefExpression> p2_l_orderkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 0, 0);
    std::shared_ptr<BoundColumnRefExpression> p2_l_suppkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 1, 0);
    std::vector<std::shared_ptr<Expression>> p2_expressions = {p2_l_orderkey, p2_l_suppkey};
    // <l_orderkey, l_suppkey>
    std::shared_ptr<PhysicalProject> project2 = std::make_shared<PhysicalProject>(p2_expressions);
    project2->exp_name = "2";

    std::vector<int> build_ids5 = {0,1};
    std::vector<int> prode_ids5 = {5,6};
    // <n_nationkey, n_name, s_suppkey, s_name, s_nationkey, l_orderkey, l_suppkey, o_orderkey, o_orderstatus>
    std::shared_ptr<PhysicalHashJoin> join5 = std::make_shared<PhysicalHashJoin>(JoinTypes::ANTI, build_ids5, prode_ids5, types);
    join5->exp_name = "5";

    std::shared_ptr<BoundColumnRefExpression> p3_s_name = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 3, 0);
    std::vector<std::shared_ptr<Expression>> p3_expressions = {p3_s_name};
    // <s_name>
    std::shared_ptr<PhysicalProject> project3 = std::make_shared<PhysicalProject>(p3_expressions);
    project3->exp_name = "3";

    // ================================================构建HashAgg================================================
    // 构建HashAgg
    std::vector<int> group_set = {0};
    std::vector<int> agg_set = {0};
    std::vector<int> star_bitmap = {1};
    std::vector<AggFunctionType> aggregate_function_types = {AggFunctionType::COUNT};
    // <s_name, Count(*)>
    std::shared_ptr<PhysicalMultiFieldHashAgg> hashAgg1 = std::make_shared<PhysicalMultiFieldHashAgg>(group_set, agg_set, star_bitmap, aggregate_function_types);

    //  ================================================构建OrderBy================================================
    std::vector<int> sort_keys = {1, 0};
    std::vector<LogicalType> key_types = {LogicalType::DOUBLE, LogicalType::STRING};
    std::vector<SortOrder> orders_p = {SortOrder::DESCENDING, SortOrder::ASCENDING};
    std::shared_ptr<PhysicalOrder> orderby = std::make_shared<PhysicalOrder>(&sort_keys, &key_types, &orders_p);

    orderby->AddChild(hashAgg1);
    hashAgg1->AddChild(project3);
    project3->AddChild(join5);
    join5->AddChild(project2);
    project2->AddChild(filter4);
    filter4->AddChild(scan_lineitem3);
    join5->AddChild(join4);
    join4->AddChild(scan_lineitem2);
    join4->AddChild(join3);
    join3->AddChild(join2);
    join3->AddChild(filter3);
    filter3->AddChild(scan_orders);
    join2->AddChild(join1);
    join2->AddChild(project1);
    project1->AddChild(filter2);
    filter2->AddChild(scan_lineitem1);
    join1->AddChild(filter1);
    join1->AddChild(scan_supplier);
    filter1->AddChild(scan_nation);

    std::shared_ptr<Pipeline> pipeline = std::make_shared<Pipeline>();
    std::shared_ptr<PipelineGroup> pipeline_group = std::make_shared<PipelineGroup>();
    orderby->build_pipelines(pipeline, pipeline_group);
    std::shared_ptr<PipelineGroupExecute> pipeline_group_executor = std::make_shared<PipelineGroupExecute>(scheduler, pipeline_group);
    pipeline_group_executor->traverse_plan();
    pipeline_group_executor->execute();
    spdlog::info("[{} : {}] Q21执行时间: {} ms.", __FILE__, __LINE__, pipeline_group_executor->duration.count());
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