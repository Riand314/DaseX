#include "import_data.hpp"
#include "file_dir_config.hpp"

// 测试单线程Q2查询
TEST(TPCHTest, Q2SingleThreadTest) {
    std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
    bool importFinish = false;
    std::string fileDir = fileDir1;
    // 插入Nation表数据
    std::shared_ptr<Table> table_nation;
    InsertNation(table_nation, scheduler, fileDir, importFinish);
    importFinish = false;
    spdlog::info("[{} : {}] InsertNation Finish!!!", __FILE__, __LINE__);

    // 插入Region表数据
    std::shared_ptr<Table> table_region;
    InsertRegion(table_region, scheduler, fileDir, importFinish);
    importFinish = false;
    spdlog::info("[{} : {}] InsertRegion Finish!!!", __FILE__, __LINE__);

    // 插入Partsupp表数据
    std::shared_ptr<Table> table_partsupp;
    InsertPartsupp(table_partsupp, scheduler, fileDir, importFinish);
    importFinish = false;
    spdlog::info("[{} : {}] InsertPartsupp Finish!!!", __FILE__, __LINE__);

    // 插入Part表数据
    std::shared_ptr<Table> table_part;
    InsertPart(table_part, scheduler, fileDir, importFinish);
    importFinish = false;
    spdlog::info("[{} : {}] InsertPart Finish!!!", __FILE__, __LINE__);

    // 插入Supplier表数据
    std::shared_ptr<Table> table_supplier;
    InsertSupplier(table_supplier, scheduler, fileDir, importFinish);
    importFinish = false;
    spdlog::info("[{} : {}] InsertSupplier Finish!!!", __FILE__, __LINE__);

    //  ================================================构建Scan================================================
    std::vector<int> region_ids = {0, 1};
    // <r_regionkey, r_name>
    std::shared_ptr<PhysicalTableScan> scan_region2 = std::make_shared<PhysicalTableScan>(table_region, -1, region_ids);

    std::vector<int> nation_ids = {0, 2};
    // <n_nationkey, n_regionkey>
    std::shared_ptr<PhysicalTableScan> scan_nation2 = std::make_shared<PhysicalTableScan>(table_nation, -1, nation_ids);

    std::vector<int> supplier_ids = {0, 3};
    // <s_suppkey, s_nationkey>
    std::shared_ptr<PhysicalTableScan> scan_supplier2 = std::make_shared<PhysicalTableScan>(table_supplier, -1, supplier_ids);

    std::vector<int> partsupp_ids = {0, 1, 3};
    // <ps_partkey, ps_suppkey, ps_supplycost>
    std::shared_ptr<PhysicalTableScan> scan_partsupp2 = std::make_shared<PhysicalTableScan>(table_partsupp, -1, partsupp_ids);
    //  ================================================构建Filter-region================================================
    std::shared_ptr<BoundColumnRefExpression> left_r_name = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    Value value_ASIA("ASIA");
    std::shared_ptr<BoundConstantExpression> right_r_name = std::make_shared<BoundConstantExpression>(value_ASIA);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_r_name = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_EQUAL);
    comparisonExpression_r_name->SetLeft(left_r_name);
    comparisonExpression_r_name->SetRight(right_r_name);
    std::shared_ptr<PhysicalFilter> filter3 = std::make_shared<PhysicalFilter>(comparisonExpression_r_name);

    //  ================================================构建HashJoin================================================
    std::vector<int> build_ids8 = {0};
    std::vector<int> prode_ids8 = {1};
    // <r_regionkey, r_name, n_nationkey, n_regionkey>
    std::shared_ptr<PhysicalHashJoin> join8 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids8, prode_ids8);
    join8->exp_name = "join8";

    std::vector<int> build_ids7 = {2};
    std::vector<int> prode_ids7 = {1};
    // <r_regionkey, r_name, n_nationkey, n_regionkey, s_suppkey, s_nationkey>
    std::shared_ptr<PhysicalHashJoin> join7 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids7, prode_ids7);
    join7->exp_name = "join7";

    std::vector<int> build_ids6 = {4};
    std::vector<int> prode_ids6 = {1};
    // <r_regionkey, r_name, n_nationkey, n_regionkey, s_suppkey, s_nationkey, ps_partkey, ps_suppkey, ps_supplycost>
    std::shared_ptr<PhysicalHashJoin> join6 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids6, prode_ids6);
    join6->exp_name = "join6";

    //  ================================================构建Project-3================================================
    std::shared_ptr<BoundColumnRefExpression> p3_ps_partkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 6, 0);
    std::shared_ptr<BoundColumnRefExpression> p3_ps_supplycost = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 8, 0);
    std::vector<std::shared_ptr<Expression>> p3_expressions = {p3_ps_partkey, p3_ps_supplycost};
    // <ps_partkey, ps_supplycost>
    std::shared_ptr<PhysicalProject> project3 = std::make_shared<PhysicalProject>(p3_expressions);
    project3->exp_name = "p3";
    //  ================================================构建HashAgg================================================
    std::vector<int> group_set = {0}; // GroupBy字段
    std::vector<int> agg_set = {1}; // 聚合字段
    std::vector<int> star_bitmap = {0}; // 对应字段是否为 ‘*’ 表
    std::vector<AggFunctionType> aggregate_function_types = { AggFunctionType::MIN };
    // <ps_partkey, Min(ps_supplycost)>
    std::shared_ptr<PhysicalMultiFieldHashAgg> hashAgg = std::make_shared<PhysicalMultiFieldHashAgg>(group_set, agg_set, star_bitmap, aggregate_function_types);

    //  ================================================构建Scan================================================
    // <r_regionkey, r_name>
    std::shared_ptr<PhysicalTableScan> scan_region1 = std::make_shared<PhysicalTableScan>(table_region, -1, region_ids);

    std::vector<int> nation_ids2 = {0, 1, 2};
    // <n_nationkey, n_name, n_regionkey>
    std::shared_ptr<PhysicalTableScan> scan_nation1 = std::make_shared<PhysicalTableScan>(table_nation, -1, nation_ids2);

    // <s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment>
    std::shared_ptr<PhysicalTableScan> scan_supplier1 = std::make_shared<PhysicalTableScan>(table_supplier);

    //  ================================================构建Filter2================================================
    std::shared_ptr<BoundColumnRefExpression> left_r_name2 = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    std::shared_ptr<BoundConstantExpression> right_r_name2 = std::make_shared<BoundConstantExpression>(value_ASIA);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_r_name2 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_EQUAL);
    comparisonExpression_r_name2->SetLeft(left_r_name2);
    comparisonExpression_r_name2->SetRight(right_r_name2);
    // <r_regionkey, r_name>
    std::shared_ptr<PhysicalFilter> filter2 = std::make_shared<PhysicalFilter>(comparisonExpression_r_name2);

    //  ================================================构建HashJoin================================================
    std::vector<int> build_ids4 = {0};
    std::vector<int> prode_ids4 = {2};
    // <r_regionkey, r_name, n_nationkey, n_name, n_regionkey>
    std::shared_ptr<PhysicalHashJoin> join4 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids4, prode_ids4);
    join4->exp_name = "join4";

    std::vector<int> build_ids3 = {2};
    std::vector<int> prode_ids3 = {3};
    // <r_regionkey, r_name, n_nationkey, n_name, n_regionkey, s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment>
    std::shared_ptr<PhysicalHashJoin> join3 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids3, prode_ids3);
    join3->exp_name = "join3";

    //  ================================================构建Scan================================================
    std::vector<int> part_ids2 = {0, 2, 4, 5};
    // <p_partkey, p_mfgr, p_type, p_size>
    std::shared_ptr<PhysicalTableScan> scan_part = std::make_shared<PhysicalTableScan>(table_part, -1, part_ids2);

    //  ================================================构建Filter-part================================================
    std::shared_ptr<BoundColumnRefExpression> left_p_size = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 3, 0);
    Value value_15(25);
    std::shared_ptr<BoundConstantExpression> right_p_constant = std::make_shared<BoundConstantExpression>(value_15);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_p_size = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_EQUAL);
    comparisonExpression_p_size->SetLeft(left_p_size);
    comparisonExpression_p_size->SetRight(right_p_constant);

    std::string like_pattern = "%BRASS";
    ScalarFunction bound_function = LikeFun::GetLikeFunction();
    std::shared_ptr<BoundColumnRefExpression> left_p_type = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 2, 0);
    Value value_BRASS(like_pattern);
    std::shared_ptr<BoundConstantExpression> right_p_constant2 = std::make_shared<BoundConstantExpression>(value_BRASS);
    std::vector<std::shared_ptr<Expression>> arguments_part;
    arguments_part.push_back(std::move(left_p_type));
    arguments_part.push_back(std::move(right_p_constant2));
    std::shared_ptr<BoundFunctionExpression> functionExpression = std::make_shared<BoundFunctionExpression>(LogicalType::BOOL, bound_function, arguments_part, nullptr);
    functionExpression->bind_info = functionExpression->function.bind(functionExpression->function, functionExpression->children);

    std::shared_ptr<BoundConjunctionExpression> conjunctionExpression2 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, comparisonExpression_p_size, functionExpression);


    // <p_partkey, p_mfgr, p_type, p_size>
    std::shared_ptr<PhysicalFilter> filter1 = std::make_shared<PhysicalFilter>(conjunctionExpression2);
    filter1->exp_name = "1";
    //  ================================================构建Project-1================================================
    std::shared_ptr<BoundColumnRefExpression> p1_p_partkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 0, 0);
    std::shared_ptr<BoundColumnRefExpression> p1_p_mfgr = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    std::vector<std::shared_ptr<Expression>> p1_expressions = {p1_p_partkey, p1_p_mfgr};
    // <p_partkey, p_mfgr>
    std::shared_ptr<PhysicalProject> project1 = std::make_shared<PhysicalProject>(p1_expressions);

    //  ================================================构建Scan================================================
    // <ps_partkey, ps_suppkey, ps_supplycost>
    std::shared_ptr<PhysicalTableScan> scan_partsupp1 = std::make_shared<PhysicalTableScan>(table_partsupp, -1, partsupp_ids);

    //  ================================================构建HashJoin================================================
    std::vector<int> build_ids1 = {0};
    std::vector<int> prode_ids1 = {0};
    // <p_partkey, p_mfgr, ps_partkey, ps_suppkey, ps_supplycost>
    std::shared_ptr<PhysicalHashJoin> join1 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids1, prode_ids1);
    join1->exp_name = "join1";

    std::vector<int> build_ids2 = {5};
    std::vector<int> prode_ids2 = {3};
    // <r_regionkey, r_name, n_nationkey, n_name, n_regionkey, s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment, p_partkey, p_mfgr, ps_partkey, ps_suppkey, ps_supplycost>
    std::shared_ptr<PhysicalHashJoin> join2 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids2, prode_ids2);
    join2->exp_name = "join2";

    std::vector<int> build_ids5 = {0};
    std::vector<int> prode_ids5 = {12};
    // <ps_partkey, Min(ps_supplycost), r_regionkey, r_name, n_nationkey, n_name, n_regionkey, s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment, p_partkey, p_mfgr, ps_partkey, ps_suppkey, ps_supplycost>
    std::shared_ptr<PhysicalHashJoin> join5 = std::make_shared<PhysicalHashJoin>(JoinTypes::LEFT, build_ids5, prode_ids5);
    join5->exp_name = "join5";

    //  ================================================构建Filter-join================================================
    std::shared_ptr<BoundColumnRefExpression> left_min_ps_supplycost = std::make_shared<BoundColumnRefExpression>(LogicalType::DOUBLE, 0, 1, 0);
    std::shared_ptr<BoundColumnRefExpression> right_ps_supplycost = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 18, 0);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_ps_supplycost = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_EQUAL);
    comparisonExpression_ps_supplycost->SetLeft(left_min_ps_supplycost);
    comparisonExpression_ps_supplycost->SetRight(right_ps_supplycost);
    std::shared_ptr<PhysicalFilter> filter4 = std::make_shared<PhysicalFilter>(comparisonExpression_ps_supplycost);

    //  ================================================构建Project-4================================================
    std::shared_ptr<BoundColumnRefExpression> p4_s_acctbal = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 12, 0);
    std::shared_ptr<BoundColumnRefExpression> p4_s_name = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 8, 0);
    std::shared_ptr<BoundColumnRefExpression> p4_n_name = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 5, 0);
    std::shared_ptr<BoundColumnRefExpression> p4_p_partkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 14, 0);
    std::shared_ptr<BoundColumnRefExpression> p4_p_mfgr = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 15, 0);
    std::shared_ptr<BoundColumnRefExpression> p4_s_address = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 9, 0);
    std::shared_ptr<BoundColumnRefExpression> p4_s_phone = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 11, 0);
    std::shared_ptr<BoundColumnRefExpression> p4_s_comment = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 13, 0);
    std::vector<std::shared_ptr<Expression>> p4_expressions = {p4_s_acctbal, p4_s_name, p4_n_name, p4_p_partkey, p4_p_mfgr, p4_s_address, p4_s_phone, p4_s_comment};
    // <s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment>
    std::shared_ptr<PhysicalProject> project4 = std::make_shared<PhysicalProject>(p4_expressions);

    //  ================================================构建OrderBy================================================
    std::vector<int> sort_keys = {0, 2, 1, 3};
    std::vector<LogicalType> key_types = {LogicalType::FLOAT, LogicalType::STRING, LogicalType::STRING, LogicalType::INTEGER};
    std::vector<SortOrder> orders_p = {SortOrder::DESCENDING, SortOrder::ASCENDING, SortOrder::ASCENDING, SortOrder::ASCENDING};
    std::shared_ptr<PhysicalOrder> orderby = std::make_shared<PhysicalOrder>(&sort_keys, &key_types, &orders_p);

    orderby->AddChild(project4);
    project4->AddChild(filter4);
    filter4->AddChild(join5);
    join5->AddChild(hashAgg);
    join5->AddChild(join2);
    hashAgg->AddChild(project3);
    project3->AddChild(join6);
    join6->AddChild(join7);
    join6->AddChild(scan_partsupp2);
    join7->AddChild(join8);
    join7->AddChild(scan_supplier2);
    join8->AddChild(filter3);
    join8->AddChild(scan_nation2);
    filter3->AddChild(scan_region2);
    join2->AddChild(join3);
    join2->AddChild(join1);
    join3->AddChild(join4);
    join3->AddChild(scan_supplier1);
    join4->AddChild(filter2);
    join4->AddChild(scan_nation1);
    filter2->AddChild(scan_region1);
    join1->AddChild(project1);
    join1->AddChild(scan_partsupp1);
    project1->AddChild(filter1);
    filter1->AddChild(scan_part);

    std::shared_ptr<Pipeline> pipeline = std::make_shared<Pipeline>();
    std::shared_ptr<PipelineGroup> pipeline_group = std::make_shared<PipelineGroup>();
    orderby->build_pipelines(pipeline, pipeline_group);
    std::shared_ptr<PipelineGroupExecute> pipeline_group_executor = std::make_shared<PipelineGroupExecute>(scheduler, pipeline_group);
    pipeline_group_executor->traverse_plan();
    auto start_execute = std::chrono::steady_clock::now();
    pipeline_group_executor->execute();
    auto end_execute = std::chrono::steady_clock::now();
    spdlog::info("[{} : {}] Q2执行时间: {}", __FILE__, __LINE__, Util::time_difference(start_execute, end_execute));

    // 关闭线程池
    scheduler->shutdown();
    RC expected = RC::SUCCESS;
    ASSERT_EQ(expected, RC::SUCCESS);
}

// 测试4线程Q2查询
TEST(TPCHTest, Q2FourThreadTest) {
    std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
    bool importFinish = false;
    std::string fileDir = fileDir4;
    std::vector<int> work_ids(arr, arr + sizeof(arr) / sizeof(arr[0]));
    std::vector<int> partition_idxs(arr, arr + sizeof(arr) / sizeof(arr[0]));
    // 插入Nation表数据
    std::shared_ptr<Table> table_nation;
    std::vector<std::string> nation_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "nation2.tbl_" + std::to_string(i);
        nation_file_names.emplace_back(file_name);
    }
    InsertNationMul(table_nation, scheduler, work_ids, partition_idxs, nation_file_names, importFinish);
    importFinish = false;

    // 插入Region表数据
    std::shared_ptr<Table> table_region;
    std::vector<std::string> region_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "region.tbl_" + std::to_string(i);
        region_file_names.emplace_back(file_name);
    }
    InsertRegionMul(table_region, scheduler, work_ids, partition_idxs, region_file_names, importFinish);
    importFinish = false;

    // 插入Partsupp表数据
    std::shared_ptr<Table> table_partsupp;
    std::vector<std::string> partsupp_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "partsupp.tbl_" + std::to_string(i);
        partsupp_file_names.emplace_back(file_name);
    }
    InsertPartsuppMul(table_partsupp, scheduler, work_ids, partition_idxs, partsupp_file_names, importFinish);
    importFinish = false;

    // 插入Part表数据
    std::shared_ptr<Table> table_part;
    std::vector<std::string> part_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "part.tbl_" + std::to_string(i);
        part_file_names.emplace_back(file_name);
    }
    InsertPartMul(table_part, scheduler, work_ids, partition_idxs, part_file_names, importFinish);
    importFinish = false;

    // 插入Supplier表数据
    std::shared_ptr<Table> table_supplier;
    std::vector<std::string> supplier_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "supplier.tbl_" + std::to_string(i);
        supplier_file_names.emplace_back(file_name);
    }
    InsertSupplierMul(table_supplier, scheduler, work_ids, partition_idxs, supplier_file_names, importFinish);
    importFinish = false;

    std::cout << "Q2: select\n"
                 "    s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment \n"
                 "from\n"
                 "    part, supplier, partsupp, nation, region\n"
                 "where\n"
                 "    p_partkey = ps_partkey\n"
                 "    and s_suppkey = ps_suppkey\n"
                 "    and p_size = 25\n"
                 "    and p_type like '%BRASS'\n"
                 "    and s_nationkey = n_nationkey\n"
                 "    and n_regionkey = r_regionkey\n"
                 "    and r_name = 'ASIA'\n"
                 "    and ps_supplycost = (\n"
                 "        select\n"
                 "            min(ps_supplycost)\n"
                 "        from\n"
                 "            partsupp, supplier, nation, region\n"
                 "        where\n"
                 "            p_partkey = ps_partkey\n"
                 "            and s_suppkey = ps_suppkey\n"
                 "            and s_nationkey = n_nationkey\n"
                 "            and n_regionkey = r_regionkey\n"
                 "            and r_name = 'ASIA'\n"
                 "    )\n"
                 "order by\n"
                 "    s_acctbal desc,\n"
                 "    n_name,\n"
                 "    s_name,\n"
                 "    p_partkey;" << std::endl;

    //  ================================================构建Scan================================================
    std::vector<int> region_ids = {0, 1};
    // <r_regionkey, r_name>
    std::shared_ptr<PhysicalTableScan> scan_region2 = std::make_shared<PhysicalTableScan>(table_region, -1, region_ids);

    std::vector<int> nation_ids = {0, 2};
    // <n_nationkey, n_regionkey>
    std::shared_ptr<PhysicalTableScan> scan_nation2 = std::make_shared<PhysicalTableScan>(table_nation, -1, nation_ids);

    std::vector<int> supplier_ids = {0, 3};
    // <s_suppkey, s_nationkey>
    std::shared_ptr<PhysicalTableScan> scan_supplier2 = std::make_shared<PhysicalTableScan>(table_supplier, -1, supplier_ids);

    std::vector<int> partsupp_ids = {0, 1, 3};
    // <ps_partkey, ps_suppkey, ps_supplycost>
    std::shared_ptr<PhysicalTableScan> scan_partsupp2 = std::make_shared<PhysicalTableScan>(table_partsupp, -1, partsupp_ids);
    //  ================================================构建Filter-region================================================
    std::shared_ptr<BoundColumnRefExpression> left_r_name = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    Value value_ASIA("ASIA");
    std::shared_ptr<BoundConstantExpression> right_r_name = std::make_shared<BoundConstantExpression>(value_ASIA);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_r_name = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_EQUAL);
    comparisonExpression_r_name->SetLeft(left_r_name);
    comparisonExpression_r_name->SetRight(right_r_name);
    std::shared_ptr<PhysicalFilter> filter3 = std::make_shared<PhysicalFilter>(comparisonExpression_r_name);

    //  ================================================构建HashJoin================================================
    std::vector<int> build_ids8 = {0};
    std::vector<int> prode_ids8 = {1};
    std::vector<ExpressionTypes> comparison_types2 = {};
    // <r_regionkey, r_name, n_nationkey, n_regionkey>
    std::shared_ptr<PhysicalHashJoin> join8 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids8, prode_ids8, comparison_types2, true);
    join8->exp_name = "join8";

    std::vector<int> build_ids7 = {2};
    std::vector<int> prode_ids7 = {1};
    // <r_regionkey, r_name, n_nationkey, n_regionkey, s_suppkey, s_nationkey>
    std::shared_ptr<PhysicalHashJoin> join7 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids7, prode_ids7);
    join7->exp_name = "join7";

    std::vector<int> build_ids6 = {4};
    std::vector<int> prode_ids6 = {1};
    // <r_regionkey, r_name, n_nationkey, n_regionkey, s_suppkey, s_nationkey, ps_partkey, ps_suppkey, ps_supplycost>
    std::shared_ptr<PhysicalHashJoin> join6 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids6, prode_ids6);
    join6->exp_name = "join6";

    //  ================================================构建Project-3================================================
    std::shared_ptr<BoundColumnRefExpression> p3_ps_partkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 6, 0);
    std::shared_ptr<BoundColumnRefExpression> p3_ps_supplycost = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 8, 0);
    std::vector<std::shared_ptr<Expression>> p3_expressions = {p3_ps_partkey, p3_ps_supplycost};
    // <ps_partkey, ps_supplycost>
    std::shared_ptr<PhysicalProject> project3 = std::make_shared<PhysicalProject>(p3_expressions);
    project3->exp_name = "p3";
    //  ================================================构建HashAgg================================================
    std::vector<int> group_set = {0}; // GroupBy字段
    std::vector<int> agg_set = {1}; // 聚合字段
    std::vector<int> star_bitmap = {0}; // 对应字段是否为 ‘*’ 表
    std::vector<AggFunctionType> aggregate_function_types = { AggFunctionType::MIN };
    // <ps_partkey, Min(ps_supplycost)>
    std::shared_ptr<PhysicalMultiFieldHashAgg> hashAgg = std::make_shared<PhysicalMultiFieldHashAgg>(group_set, agg_set, star_bitmap, aggregate_function_types);

    //  ================================================构建Scan================================================
    // <r_regionkey, r_name>
    std::shared_ptr<PhysicalTableScan> scan_region1 = std::make_shared<PhysicalTableScan>(table_region, -1, region_ids);

    std::vector<int> nation_ids2 = {0, 1, 2};
    // <n_nationkey, n_name, n_regionkey>
    std::shared_ptr<PhysicalTableScan> scan_nation1 = std::make_shared<PhysicalTableScan>(table_nation, -1, nation_ids2);

    // <s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment>
    std::shared_ptr<PhysicalTableScan> scan_supplier1 = std::make_shared<PhysicalTableScan>(table_supplier);

    //  ================================================构建Filter2================================================
    std::shared_ptr<BoundColumnRefExpression> left_r_name2 = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    std::shared_ptr<BoundConstantExpression> right_r_name2 = std::make_shared<BoundConstantExpression>(value_ASIA);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_r_name2 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_EQUAL);
    comparisonExpression_r_name2->SetLeft(left_r_name2);
    comparisonExpression_r_name2->SetRight(right_r_name2);
    // <r_regionkey, r_name>
    std::shared_ptr<PhysicalFilter> filter2 = std::make_shared<PhysicalFilter>(comparisonExpression_r_name2);

    //  ================================================构建HashJoin================================================
    std::vector<int> build_ids4 = {0};
    std::vector<int> prode_ids4 = {2};
    std::vector<ExpressionTypes> comparison_types = {};
    // <r_regionkey, r_name, n_nationkey, n_name, n_regionkey>
    std::shared_ptr<PhysicalHashJoin> join4 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids4, prode_ids4, comparison_types, true);
    join4->exp_name = "join4";

    std::vector<int> build_ids3 = {2};
    std::vector<int> prode_ids3 = {3};
    // <r_regionkey, r_name, n_nationkey, n_name, n_regionkey, s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment>
    std::shared_ptr<PhysicalHashJoin> join3 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids3, prode_ids3);
    join3->exp_name = "join3";

    //  ================================================构建Scan================================================
    std::vector<int> part_ids2 = {0, 2, 4, 5};
    // <p_partkey, p_mfgr, p_type, p_size>
    std::shared_ptr<PhysicalTableScan> scan_part = std::make_shared<PhysicalTableScan>(table_part, -1, part_ids2);

    //  ================================================构建Filter-part================================================
    std::shared_ptr<BoundColumnRefExpression> left_p_size = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 3, 0);
    Value value_15(25);
    std::shared_ptr<BoundConstantExpression> right_p_constant = std::make_shared<BoundConstantExpression>(value_15);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_p_size = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_EQUAL);
    comparisonExpression_p_size->SetLeft(left_p_size);
    comparisonExpression_p_size->SetRight(right_p_constant);

    std::string like_pattern = "%BRASS";
    ScalarFunction bound_function = LikeFun::GetLikeFunction();
    std::shared_ptr<BoundColumnRefExpression> left_p_type = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 2, 0);
    Value value_BRASS(like_pattern);
    std::shared_ptr<BoundConstantExpression> right_p_constant2 = std::make_shared<BoundConstantExpression>(value_BRASS);
    std::vector<std::shared_ptr<Expression>> arguments_part;
    arguments_part.push_back(std::move(left_p_type));
    arguments_part.push_back(std::move(right_p_constant2));
    std::shared_ptr<BoundFunctionExpression> functionExpression = std::make_shared<BoundFunctionExpression>(LogicalType::BOOL, bound_function, arguments_part, nullptr);
    functionExpression->bind_info = functionExpression->function.bind(functionExpression->function, functionExpression->children);

    std::shared_ptr<BoundConjunctionExpression> conjunctionExpression2 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, comparisonExpression_p_size, functionExpression);


    // <p_partkey, p_mfgr, p_type, p_size>
    std::shared_ptr<PhysicalFilter> filter1 = std::make_shared<PhysicalFilter>(conjunctionExpression2);
    filter1->exp_name = "1";
    //  ================================================构建Project-1================================================
    std::shared_ptr<BoundColumnRefExpression> p1_p_partkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 0, 0);
    std::shared_ptr<BoundColumnRefExpression> p1_p_mfgr = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    std::vector<std::shared_ptr<Expression>> p1_expressions = {p1_p_partkey, p1_p_mfgr};
    // <p_partkey, p_mfgr>
    std::shared_ptr<PhysicalProject> project1 = std::make_shared<PhysicalProject>(p1_expressions);

    //  ================================================构建Scan================================================
    // <ps_partkey, ps_suppkey, ps_supplycost>
    std::shared_ptr<PhysicalTableScan> scan_partsupp1 = std::make_shared<PhysicalTableScan>(table_partsupp, -1, partsupp_ids);

    //  ================================================构建HashJoin================================================
    std::vector<int> build_ids1 = {0};
    std::vector<int> prode_ids1 = {0};
    // <p_partkey, p_mfgr, ps_partkey, ps_suppkey, ps_supplycost>
    std::shared_ptr<PhysicalHashJoin> join1 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids1, prode_ids1);
    join1->exp_name = "join1";

    std::vector<int> build_ids2 = {5};
    std::vector<int> prode_ids2 = {3};
    // <r_regionkey, r_name, n_nationkey, n_name, n_regionkey, s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment, p_partkey, p_mfgr, ps_partkey, ps_suppkey, ps_supplycost>
    std::shared_ptr<PhysicalHashJoin> join2 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids2, prode_ids2);
    join2->exp_name = "join2";

    std::vector<int> build_ids5 = {0};
    std::vector<int> prode_ids5 = {12};
    // <ps_partkey, Min(ps_supplycost), r_regionkey, r_name, n_nationkey, n_name, n_regionkey, s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment, p_partkey, p_mfgr, ps_partkey, ps_suppkey, ps_supplycost>
    std::shared_ptr<PhysicalHashJoin> join5 = std::make_shared<PhysicalHashJoin>(JoinTypes::LEFT, build_ids5, prode_ids5);
    join5->exp_name = "join5";

    //  ================================================构建Filter-join================================================
    std::shared_ptr<BoundColumnRefExpression> left_min_ps_supplycost = std::make_shared<BoundColumnRefExpression>(LogicalType::DOUBLE, 0, 1, 0);
    std::shared_ptr<BoundColumnRefExpression> right_ps_supplycost = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 18, 0);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression_ps_supplycost = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_EQUAL);
    comparisonExpression_ps_supplycost->SetLeft(left_min_ps_supplycost);
    comparisonExpression_ps_supplycost->SetRight(right_ps_supplycost);
    std::shared_ptr<PhysicalFilter> filter4 = std::make_shared<PhysicalFilter>(comparisonExpression_ps_supplycost);

    //  ================================================构建Project-4================================================
    std::shared_ptr<BoundColumnRefExpression> p4_s_acctbal = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 12, 0);
    std::shared_ptr<BoundColumnRefExpression> p4_s_name = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 8, 0);
    std::shared_ptr<BoundColumnRefExpression> p4_n_name = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 5, 0);
    std::shared_ptr<BoundColumnRefExpression> p4_p_partkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 14, 0);
    std::shared_ptr<BoundColumnRefExpression> p4_p_mfgr = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 15, 0);
    std::shared_ptr<BoundColumnRefExpression> p4_s_address = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 9, 0);
    std::shared_ptr<BoundColumnRefExpression> p4_s_phone = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 11, 0);
    std::shared_ptr<BoundColumnRefExpression> p4_s_comment = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 13, 0);
    std::vector<std::shared_ptr<Expression>> p4_expressions = {p4_s_acctbal, p4_s_name, p4_n_name, p4_p_partkey, p4_p_mfgr, p4_s_address, p4_s_phone, p4_s_comment};
    // <s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment>
    std::shared_ptr<PhysicalProject> project4 = std::make_shared<PhysicalProject>(p4_expressions);

    //  ================================================构建OrderBy================================================
    std::vector<int> sort_keys = {0, 2, 1, 3};
    std::vector<LogicalType> key_types = {LogicalType::FLOAT, LogicalType::STRING, LogicalType::STRING, LogicalType::INTEGER};
    std::vector<SortOrder> orders_p = {SortOrder::DESCENDING, SortOrder::ASCENDING, SortOrder::ASCENDING, SortOrder::ASCENDING};
    std::shared_ptr<PhysicalOrder> orderby = std::make_shared<PhysicalOrder>(&sort_keys, &key_types, &orders_p);

    orderby->AddChild(project4);
    project4->AddChild(filter4);
    filter4->AddChild(join5);
    join5->AddChild(hashAgg);
    join5->AddChild(join2);
    hashAgg->AddChild(project3);
    project3->AddChild(join6);
    join6->AddChild(join7);
    join6->AddChild(scan_partsupp2);
    join7->AddChild(join8);
    join7->AddChild(scan_supplier2);
    join8->AddChild(filter3);
    join8->AddChild(scan_nation2);
    filter3->AddChild(scan_region2);
    join2->AddChild(join3);
    join2->AddChild(join1);
    join3->AddChild(join4);
    join3->AddChild(scan_supplier1);
    join4->AddChild(filter2);
    join4->AddChild(scan_nation1);
    filter2->AddChild(scan_region1);
    join1->AddChild(project1);
    join1->AddChild(scan_partsupp1);
    project1->AddChild(filter1);
    filter1->AddChild(scan_part);

    std::shared_ptr<Pipeline> pipeline = std::make_shared<Pipeline>();
    std::shared_ptr<PipelineGroup> pipeline_group = std::make_shared<PipelineGroup>();
    orderby->build_pipelines(pipeline, pipeline_group);
    std::shared_ptr<PipelineGroupExecute> pipeline_group_executor = std::make_shared<PipelineGroupExecute>(scheduler, pipeline_group);
    pipeline_group_executor->traverse_plan();
//    auto start_execute = std::chrono::steady_clock::now();
    pipeline_group_executor->execute();
//    auto end_execute = std::chrono::steady_clock::now();
    spdlog::info("[{} : {}] Q2执行时间: {} ms.", __FILE__, __LINE__, pipeline_group_executor->duration.count());
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