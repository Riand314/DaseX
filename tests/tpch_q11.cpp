#include "import_data.hpp"
#include "file_dir_config.hpp"
// =============================Q11=============================
// 测试单线程Q11查询
TEST(TPCHTest, Q11SingleThreadTest) {
    std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
    bool importFinish = false;
    std::string fileDir = fileDir1;
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

    // 插入Partsupp表数据
    std::shared_ptr<Table> table_partsupp;
    InsertPartsupp(table_partsupp, scheduler, fileDir, importFinish);
    importFinish = false;
    spdlog::info("[{} : {}] InsertPartsupp Finish!!!", __FILE__, __LINE__);

    //  ================================================构建Scan================================================
    std::vector<int> nation_ids = {0, 1};
    // <n_nationkey, n_name>
    std::shared_ptr<PhysicalTableScan> scan_nation1 = std::make_shared<PhysicalTableScan>(table_nation, -1, nation_ids);
    std::shared_ptr<PhysicalTableScan> scan_nation2 = std::make_shared<PhysicalTableScan>(table_nation, -1, nation_ids);

    std::vector<int> supplier_ids = {0, 3};
    // <s_suppkey, s_nationkey>
    std::shared_ptr<PhysicalTableScan> scan_supplier1 = std::make_shared<PhysicalTableScan>(table_supplier, -1, supplier_ids);
    std::shared_ptr<PhysicalTableScan> scan_supplier2 = std::make_shared<PhysicalTableScan>(table_supplier, -1, supplier_ids);

    std::vector<int> partsupp_ids = {1, 2, 3};
    // <ps_suppkey, ps_availqty, ps_supplycost>
    std::shared_ptr<PhysicalTableScan> scan_partsupp1 = std::make_shared<PhysicalTableScan>(table_partsupp, -1, partsupp_ids);

    std::vector<int> partsupp_ids2 = {0, 1, 2, 3};
    // <ps_partkey, ps_suppkey, ps_availqty, ps_supplycost>
    std::shared_ptr<PhysicalTableScan> scan_partsupp2 = std::make_shared<PhysicalTableScan>(table_partsupp, -1, partsupp_ids2);

    //  ================================================构建Filter================================================
    std::shared_ptr<BoundColumnRefExpression> f1_n_name = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    Value value_GERMANY("GERMANY");
    std::shared_ptr<BoundConstantExpression> right_constant = std::make_shared<BoundConstantExpression>(value_GERMANY);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_n_name = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_EQUAL);
    comparisonExpression_n_name->SetLeft(f1_n_name);
    comparisonExpression_n_name->SetRight(right_constant);
    // <n_nationkey, n_name>
    std::shared_ptr<PhysicalFilter> filter1 = std::make_shared<PhysicalFilter>(comparisonExpression_n_name);

    //  ================================================构建HashJoin================================================
    std::vector<int> build_ids1 = {0};
    std::vector<int> prode_ids1 = {1};
    // <n_nationkey, n_name, s_suppkey, s_nationkey>
    std::shared_ptr<PhysicalHashJoin> join1 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids1, prode_ids1);
    join1->exp_name = "1";

    std::vector<int> build_ids2 = {2};
    std::vector<int> prode_ids2 = {0};
    // <n_nationkey, n_name, s_suppkey, s_nationkey, ps_suppkey, ps_availqty, ps_supplycost>
    std::shared_ptr<PhysicalHashJoin> join2 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids2, prode_ids2);
    join2->exp_name = "2";

    //  ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p1_ps_supplycost = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 6, 0);
    std::shared_ptr<BoundColumnRefExpression> p1_ps_availqty = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 5, 0);
    std::vector<LogicalType> p1_arguments = { LogicalType::FLOAT, LogicalType::INTEGER };
    scalar_function_p p1_function = GetMulFunction();
    ScalarProjectFunction p1_bound_function("*", p1_arguments, LogicalType::FLOAT, p1_function, nullptr);
    std::vector<std::shared_ptr<Expression>> p1_expressions = {p1_ps_supplycost, p1_ps_availqty};
    std::shared_ptr<BoundProjectFunctionExpression> p1_alias = std::make_shared<BoundProjectFunctionExpression>(LogicalType::FLOAT, p1_bound_function, p1_expressions, nullptr);
    p1_alias->alias = "ps_supplycost * ps_availqty";

    std::vector<std::shared_ptr<Expression>> p1_expressions2 = {p1_alias};
    // <(ps_supplycost * ps_availqty)>
    std::shared_ptr<PhysicalProject> project1 = std::make_shared<PhysicalProject>(p1_expressions2);
    project1->exp_name = "1";

    //  ================================================构建HashAgg================================================
    std::vector<int> agg_set = {0};
    std::vector<AggFunctionType> aggregate_function_types = {AggFunctionType::SUM};
    // <Sum(ps_supplycost * ps_availqty)>
    std::shared_ptr<PhysicalHashAgg> hashAgg1 = std::make_shared<PhysicalHashAgg>(-1, aggregate_function_types, agg_set);

    //  ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p2_sum = std::make_shared<BoundColumnRefExpression>(LogicalType::DOUBLE, 0, 0, 0);
    Value value_00001(0.0001);
    std::shared_ptr<BoundConstantExpression> p2_constant = std::make_shared<BoundConstantExpression>(value_00001);
    std::vector<LogicalType> p2_arguments = { LogicalType::DOUBLE, LogicalType::DOUBLE };
    scalar_function_p p2_function = GetMulFunction();
    ScalarProjectFunction p2_bound_function("*", p2_arguments, LogicalType::DOUBLE, p2_function, nullptr);
    std::vector<std::shared_ptr<Expression>> p2_expressions = {p2_sum, p2_constant};
    std::shared_ptr<BoundProjectFunctionExpression> p2_alias = std::make_shared<BoundProjectFunctionExpression>(LogicalType::FLOAT, p2_bound_function, p2_expressions, nullptr);
    p2_alias->alias = "Sum(ps_supplycost * ps_availqty) * 0.0001";

    std::vector<std::shared_ptr<Expression>> p2_expressions2 = {p2_alias};
    // <Sum(ps_supplycost * ps_availqty) * 0.0001>
    std::shared_ptr<PhysicalProject> project2 = std::make_shared<PhysicalProject>(p2_expressions2);
    project2->exp_name = "2";
    //  ================================================构建Filter================================================
    std::shared_ptr<BoundColumnRefExpression> f2_n_name = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    std::shared_ptr<BoundConstantExpression> right_constant2 = std::make_shared<BoundConstantExpression>(value_GERMANY);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_n_name2 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_EQUAL);
    comparisonExpression_n_name2->SetLeft(f2_n_name);
    comparisonExpression_n_name2->SetRight(right_constant2);
    // <n_nationkey, n_name>
    std::shared_ptr<PhysicalFilter> filter2 = std::make_shared<PhysicalFilter>(comparisonExpression_n_name2);

    //  ================================================构建HashJoin================================================
    std::vector<int> build_ids3 = {0};
    std::vector<int> prode_ids3 = {1};
    // <n_nationkey, n_name, s_suppkey, s_nationkey>
    std::shared_ptr<PhysicalHashJoin> join3 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids3, prode_ids3);
    join3->exp_name = "3";

    std::vector<int> build_ids4 = {2};
    std::vector<int> prode_ids4 = {1};
    // <s_suppkey, s_nationkey, n_nationkey, n_name, ps_partkey, ps_suppkey, ps_availqty, ps_supplycost>
    std::shared_ptr<PhysicalHashJoin> join4 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids4, prode_ids4);
    join4->exp_name = "4";

    //  ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p3_ps_partkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 4, 0);
    std::shared_ptr<BoundColumnRefExpression> p3_ps_supplycost = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 7, 0);
    std::shared_ptr<BoundColumnRefExpression> p3_ps_availqty = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 6, 0);
    std::vector<LogicalType> p3_arguments = { LogicalType::FLOAT, LogicalType::INTEGER };
    scalar_function_p p3_function = GetMulFunction();
    ScalarProjectFunction p3_bound_function("*", p3_arguments, LogicalType::FLOAT, p3_function, nullptr);
    std::vector<std::shared_ptr<Expression>> p3_expressions = {p3_ps_supplycost, p3_ps_availqty};
    std::shared_ptr<BoundProjectFunctionExpression> p3_alias = std::make_shared<BoundProjectFunctionExpression>(LogicalType::FLOAT, p3_bound_function, p3_expressions, nullptr);
    p3_alias->alias = "ps_supplycost * ps_availqty";

    std::vector<std::shared_ptr<Expression>> p3_expressions2 = {p3_ps_partkey, p3_alias};
    // <ps_partkey, (ps_supplycost * ps_availqty)>
    std::shared_ptr<PhysicalProject> project3 = std::make_shared<PhysicalProject>(p3_expressions2);
    project3->exp_name = "3";
    // 构建HashAgg
    std::vector<int> group_set2 = {0};
    std::vector<int> agg_set2 = {1};
    std::vector<int> star_bitmap2 = {0};
    std::vector<AggFunctionType> aggregate_function_types2 = {AggFunctionType::SUM};
    // <ps_partkey, Sum(ps_supplycost * ps_availqty)-value>
    std::shared_ptr<PhysicalMultiFieldHashAgg> hashAgg2 = std::make_shared<PhysicalMultiFieldHashAgg>(group_set2, agg_set2, star_bitmap2, aggregate_function_types2);

    //  ================================================构建NestLoopJoin================================================
    std::vector<int> build_ids5 = {0};
    std::vector<int> prode_ids5 = {1};
    std::vector<ExpressionTypes> compare_types = { ExpressionTypes::COMPARE_LESSTHAN };
    // <Sum(ps_supplycost * ps_availqty) * 0.0001, ps_partkey, Sum(ps_supplycost * ps_availqty)-value>
    std::shared_ptr<PhysicalNestedLoopJoin> join5 = std::make_shared<PhysicalNestedLoopJoin>(JoinTypes::INNER, build_ids5, prode_ids5, compare_types);
    join5->exp_name = "5";

    //  ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p4_ps_partkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 1, 0);
    std::shared_ptr<BoundColumnRefExpression> p4_value = std::make_shared<BoundColumnRefExpression>(LogicalType::DOUBLE, 0, 2, 0);
    std::vector<std::shared_ptr<Expression>> p4_expressions = {p4_ps_partkey, p4_value};
    // <ps_partkey, value>
    std::shared_ptr<PhysicalProject> project4 = std::make_shared<PhysicalProject>(p4_expressions);
    //  ================================================构建OrderBy================================================
    std::vector<int> sort_keys = {1};
    std::vector<LogicalType> key_types = {LogicalType::DOUBLE};
    std::vector<SortOrder> orders_p = {SortOrder::DESCENDING};
    std::shared_ptr<PhysicalOrder> orderby = std::make_shared<PhysicalOrder>(&sort_keys, &key_types, &orders_p);

    orderby->AddChild(project4);
    project4->AddChild(join5);
    join5->AddChild(project2);
    join5->AddChild(hashAgg2);
    project2->AddChild(hashAgg1);
    hashAgg1->AddChild(project1);
    project1->AddChild(join2);
    join2->AddChild(join1);
    join2->AddChild(scan_partsupp1);
    join1->AddChild(filter1);
    join1->AddChild(scan_supplier1);
    filter1->AddChild(scan_nation1);
    hashAgg2->AddChild(project3);
    project3->AddChild(join4);
    join4->AddChild(join3);
    join4->AddChild(scan_partsupp2);
    join3->AddChild(filter2);
    join3->AddChild(scan_supplier2);
    filter2->AddChild(scan_nation2);

    std::shared_ptr<Pipeline> pipeline = std::make_shared<Pipeline>();
    std::shared_ptr<PipelineGroup> pipeline_group = std::make_shared<PipelineGroup>();
    orderby->build_pipelines(pipeline, pipeline_group);
    std::shared_ptr<PipelineGroupExecute> pipeline_group_executor = std::make_shared<PipelineGroupExecute>(scheduler, pipeline_group);
    pipeline_group_executor->traverse_plan();
    auto start_execute = std::chrono::steady_clock::now();
    pipeline_group_executor->execute();
    auto end_execute = std::chrono::steady_clock::now();
    spdlog::info("[{} : {}] Q11执行时间: {}", __FILE__, __LINE__, Util::time_difference(start_execute, end_execute));

    // 关闭线程池
    scheduler->shutdown();
    RC expected = RC::SUCCESS;
    ASSERT_EQ(expected, RC::SUCCESS);
}

// 测试4线程Q11查询
TEST(TPCHTest, Q11FourThreadTest) {
    std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
    bool importFinish = false;
    std::string fileDir = fileDir4;
    std::vector<int> work_ids(arr, arr + sizeof(arr) / sizeof(arr[0]));
    std::vector<int> partition_idxs(arr, arr + sizeof(arr) / sizeof(arr[0]));

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
    std::this_thread::sleep_for(std::chrono::seconds(1));
    // spdlog::info("[{} : {}] InsertSupplier Finish!!!", __FILE__, __LINE__);

    // 插入Partsupp表数据
    std::shared_ptr<Table> table_partsupp;
    std::vector<std::string> partsupp_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "partsupp.tbl_" + std::to_string(i);
        partsupp_file_names.emplace_back(file_name);
    }
    InsertPartsuppMul(table_partsupp, scheduler, work_ids, partition_idxs, partsupp_file_names, importFinish);
    importFinish = false;
    std::this_thread::sleep_for(std::chrono::seconds(1));
    // spdlog::info("[{} : {}] InsertPartsupp Finish!!!", __FILE__, __LINE__);

    std::cout << "Q11: select\n"
                 "    ps_partkey,\n"
                 "    sum(ps_supplycost * ps_availqty) as value\n"
                 "from\n"
                 "    partsupp, supplier, nation\n"
                 "where\n"
                 "    ps_suppkey = s_suppkey\n"
                 "    and s_nationkey = n_nationkey\n"
                 "    and n_name = 'GERMANY'\n"
                 "group by\n"
                 "    ps_partkey \n"
                 "having\n"
                 "    sum(ps_supplycost * ps_availqty) > (\n"
                 "        select\n"
                 "            sum(ps_supplycost * ps_availqty) * 0.0001\n"
                 "        from\n"
                 "            partsupp, supplier, nation\n"
                 "        where\n"
                 "            ps_suppkey = s_suppkey\n"
                 "            and s_nationkey = n_nationkey\n"
                 "            and n_name = 'GERMANY'\n"
                 "    )\n"
                 "order by\n"
                 "    value desc;" << std::endl;

    //  ================================================构建Scan================================================
    std::vector<int> nation_ids = {0, 1};
    // <n_nationkey, n_name>
    std::shared_ptr<PhysicalTableScan> scan_nation1 = std::make_shared<PhysicalTableScan>(table_nation, -1, nation_ids);
    std::shared_ptr<PhysicalTableScan> scan_nation2 = std::make_shared<PhysicalTableScan>(table_nation, -1, nation_ids);

    std::vector<int> supplier_ids = {0, 3};
    // <s_suppkey, s_nationkey>
    std::shared_ptr<PhysicalTableScan> scan_supplier1 = std::make_shared<PhysicalTableScan>(table_supplier, -1, supplier_ids);
    std::shared_ptr<PhysicalTableScan> scan_supplier2 = std::make_shared<PhysicalTableScan>(table_supplier, -1, supplier_ids);

    std::vector<int> partsupp_ids = {1, 2, 3};
    // <ps_suppkey, ps_availqty, ps_supplycost>
    std::shared_ptr<PhysicalTableScan> scan_partsupp1 = std::make_shared<PhysicalTableScan>(table_partsupp, -1, partsupp_ids);

    std::vector<int> partsupp_ids2 = {0, 1, 2, 3};
    // <ps_partkey, ps_suppkey, ps_availqty, ps_supplycost>
    std::shared_ptr<PhysicalTableScan> scan_partsupp2 = std::make_shared<PhysicalTableScan>(table_partsupp, -1, partsupp_ids2);

    //  ================================================构建Filter================================================
    std::shared_ptr<BoundColumnRefExpression> f1_n_name = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    Value value_GERMANY("GERMANY");
    std::shared_ptr<BoundConstantExpression> right_constant = std::make_shared<BoundConstantExpression>(value_GERMANY);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_n_name = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_EQUAL);
    comparisonExpression_n_name->SetLeft(f1_n_name);
    comparisonExpression_n_name->SetRight(right_constant);
    // <n_nationkey, n_name>
    std::shared_ptr<PhysicalFilter> filter1 = std::make_shared<PhysicalFilter>(comparisonExpression_n_name);

    //  ================================================构建HashJoin================================================
    std::vector<int> build_ids1 = {0};
    std::vector<int> prode_ids1 = {1};
    std::vector<ExpressionTypes> comparison_types1 = {};
    // <n_nationkey, n_name, s_suppkey, s_nationkey>
    std::shared_ptr<PhysicalHashJoin> join1 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids1, prode_ids1, comparison_types1, true);
    join1->exp_name = "1";

    std::vector<int> build_ids2 = {2};
    std::vector<int> prode_ids2 = {0};
    // <n_nationkey, n_name, s_suppkey, s_nationkey, ps_suppkey, ps_availqty, ps_supplycost>
    std::shared_ptr<PhysicalHashJoin> join2 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids2, prode_ids2);
    join2->exp_name = "2";

    //  ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p1_ps_supplycost = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 6, 0);
    std::shared_ptr<BoundColumnRefExpression> p1_ps_availqty = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 5, 0);
    std::vector<LogicalType> p1_arguments = { LogicalType::FLOAT, LogicalType::INTEGER };
    scalar_function_p p1_function = GetMulFunction();
    ScalarProjectFunction p1_bound_function("*", p1_arguments, LogicalType::FLOAT, p1_function, nullptr);
    std::vector<std::shared_ptr<Expression>> p1_expressions = {p1_ps_supplycost, p1_ps_availqty};
    std::shared_ptr<BoundProjectFunctionExpression> p1_alias = std::make_shared<BoundProjectFunctionExpression>(LogicalType::FLOAT, p1_bound_function, p1_expressions, nullptr);
    p1_alias->alias = "ps_supplycost * ps_availqty";

    std::vector<std::shared_ptr<Expression>> p1_expressions2 = {p1_alias};
    // <(ps_supplycost * ps_availqty)>
    std::shared_ptr<PhysicalProject> project1 = std::make_shared<PhysicalProject>(p1_expressions2);
    project1->exp_name = "1";

    //  ================================================构建HashAgg================================================
    std::vector<int> agg_set = {0};
    std::vector<AggFunctionType> aggregate_function_types = {AggFunctionType::SUM};
    // <Sum(ps_supplycost * ps_availqty)>
    std::shared_ptr<PhysicalHashAgg> hashAgg1 = std::make_shared<PhysicalHashAgg>(-1, aggregate_function_types, agg_set);

    //  ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p2_sum = std::make_shared<BoundColumnRefExpression>(LogicalType::DOUBLE, 0, 0, 0);
    Value value_00001(0.0001);
    std::shared_ptr<BoundConstantExpression> p2_constant = std::make_shared<BoundConstantExpression>(value_00001);
    std::vector<LogicalType> p2_arguments = { LogicalType::DOUBLE, LogicalType::DOUBLE };
    scalar_function_p p2_function = GetMulFunction();
    ScalarProjectFunction p2_bound_function("*", p2_arguments, LogicalType::DOUBLE, p2_function, nullptr);
    std::vector<std::shared_ptr<Expression>> p2_expressions = {p2_sum, p2_constant};
    std::shared_ptr<BoundProjectFunctionExpression> p2_alias = std::make_shared<BoundProjectFunctionExpression>(LogicalType::FLOAT, p2_bound_function, p2_expressions, nullptr);
    p2_alias->alias = "Sum(ps_supplycost * ps_availqty) * 0.0001";

    std::vector<std::shared_ptr<Expression>> p2_expressions2 = {p2_alias};
    // <Sum(ps_supplycost * ps_availqty) * 0.0001>
    std::shared_ptr<PhysicalProject> project2 = std::make_shared<PhysicalProject>(p2_expressions2);
    project2->exp_name = "2";
    //  ================================================构建Filter================================================
    std::shared_ptr<BoundColumnRefExpression> f2_n_name = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    std::shared_ptr<BoundConstantExpression> right_constant2 = std::make_shared<BoundConstantExpression>(value_GERMANY);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_n_name2 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_EQUAL);
    comparisonExpression_n_name2->SetLeft(f2_n_name);
    comparisonExpression_n_name2->SetRight(right_constant2);
    // <n_nationkey, n_name>
    std::shared_ptr<PhysicalFilter> filter2 = std::make_shared<PhysicalFilter>(comparisonExpression_n_name2);

    //  ================================================构建HashJoin================================================
    std::vector<int> build_ids3 = {0};
    std::vector<int> prode_ids3 = {1};
    std::vector<ExpressionTypes> comparison_types3 = {};
    // <n_nationkey, n_name, s_suppkey, s_nationkey>
    std::shared_ptr<PhysicalHashJoin> join3 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids3, prode_ids3, comparison_types3, true);
    join3->exp_name = "3";

    std::vector<int> build_ids4 = {2};
    std::vector<int> prode_ids4 = {1};
    // <s_suppkey, s_nationkey, n_nationkey, n_name, ps_partkey, ps_suppkey, ps_availqty, ps_supplycost>
    std::shared_ptr<PhysicalHashJoin> join4 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids4, prode_ids4);
    join4->exp_name = "4";

    //  ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p3_ps_partkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 4, 0);
    std::shared_ptr<BoundColumnRefExpression> p3_ps_supplycost = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 7, 0);
    std::shared_ptr<BoundColumnRefExpression> p3_ps_availqty = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 6, 0);
    std::vector<LogicalType> p3_arguments = { LogicalType::FLOAT, LogicalType::INTEGER };
    scalar_function_p p3_function = GetMulFunction();
    ScalarProjectFunction p3_bound_function("*", p3_arguments, LogicalType::FLOAT, p3_function, nullptr);
    std::vector<std::shared_ptr<Expression>> p3_expressions = {p3_ps_supplycost, p3_ps_availqty};
    std::shared_ptr<BoundProjectFunctionExpression> p3_alias = std::make_shared<BoundProjectFunctionExpression>(LogicalType::FLOAT, p3_bound_function, p3_expressions, nullptr);
    p3_alias->alias = "ps_supplycost * ps_availqty";

    std::vector<std::shared_ptr<Expression>> p3_expressions2 = {p3_ps_partkey, p3_alias};
    // <ps_partkey, (ps_supplycost * ps_availqty)>
    std::shared_ptr<PhysicalProject> project3 = std::make_shared<PhysicalProject>(p3_expressions2);
    project3->exp_name = "3";
    // 构建HashAgg
    std::vector<int> group_set2 = {0};
    std::vector<int> agg_set2 = {1};
    std::vector<int> star_bitmap2 = {0};
    std::vector<AggFunctionType> aggregate_function_types2 = {AggFunctionType::SUM};
    // <ps_partkey, Sum(ps_supplycost * ps_availqty)-value>
    std::shared_ptr<PhysicalMultiFieldHashAgg> hashAgg2 = std::make_shared<PhysicalMultiFieldHashAgg>(group_set2, agg_set2, star_bitmap2, aggregate_function_types2);

    //  ================================================构建NestLoopJoin================================================
    std::vector<int> build_ids5 = {0};
    std::vector<int> prode_ids5 = {1};
    std::vector<ExpressionTypes> compare_types = { ExpressionTypes::COMPARE_LESSTHAN };
    // <Sum(ps_supplycost * ps_availqty) * 0.0001, ps_partkey, Sum(ps_supplycost * ps_availqty)-value>
    std::shared_ptr<PhysicalNestedLoopJoin> join5 = std::make_shared<PhysicalNestedLoopJoin>(JoinTypes::INNER, build_ids5, prode_ids5, compare_types);
    join5->exp_name = "5";

    //  ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p4_ps_partkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 1, 0);
    std::shared_ptr<BoundColumnRefExpression> p4_value = std::make_shared<BoundColumnRefExpression>(LogicalType::DOUBLE, 0, 2, 0);
    std::vector<std::shared_ptr<Expression>> p4_expressions = {p4_ps_partkey, p4_value};
    // <ps_partkey, value>
    std::shared_ptr<PhysicalProject> project4 = std::make_shared<PhysicalProject>(p4_expressions);
    project4->exp_name = "4";
    //  ================================================构建OrderBy================================================
    std::vector<int> sort_keys = {1};
    std::vector<LogicalType> key_types = {LogicalType::DOUBLE};
    std::vector<SortOrder> orders_p = {SortOrder::DESCENDING};
    std::shared_ptr<PhysicalOrder> orderby = std::make_shared<PhysicalOrder>(&sort_keys, &key_types, &orders_p);

    orderby->AddChild(project4);
    project4->AddChild(join5);
    join5->AddChild(project2);
    join5->AddChild(hashAgg2);
    project2->AddChild(hashAgg1);
    hashAgg1->AddChild(project1);
    project1->AddChild(join2);
    join2->AddChild(join1);
    join2->AddChild(scan_partsupp1);
    join1->AddChild(filter1);
    join1->AddChild(scan_supplier1);
    filter1->AddChild(scan_nation1);
    hashAgg2->AddChild(project3);
    project3->AddChild(join4);
    join4->AddChild(join3);
    join4->AddChild(scan_partsupp2);
    join3->AddChild(filter2);
    join3->AddChild(scan_supplier2);
    filter2->AddChild(scan_nation2);

    std::shared_ptr<Pipeline> pipeline = std::make_shared<Pipeline>();
    std::shared_ptr<PipelineGroup> pipeline_group = std::make_shared<PipelineGroup>();
    orderby->build_pipelines(pipeline, pipeline_group);
    std::shared_ptr<PipelineGroupExecute> pipeline_group_executor = std::make_shared<PipelineGroupExecute>(scheduler, pipeline_group);
    pipeline_group_executor->traverse_plan();
    pipeline_group_executor->execute();
    spdlog::info("[{} : {}] Q11执行时间: {} ms.", __FILE__, __LINE__, pipeline_group_executor->duration.count());
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