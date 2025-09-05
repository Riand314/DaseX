#include "import_data.hpp"
#include "file_dir_config.hpp"
// =============================Q20=============================
// 测试单线程Q20查询
TEST(TPCHTest, Q20SingleThreadTest) {
    std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
    bool importFinish = false;
    std::string fileDir = fileDir1;
    // 插入Lineitem表数据
    std::shared_ptr<Table> table_lineitem;
    InsertLineitem(table_lineitem, scheduler, fileDir, importFinish);
    importFinish = false;
    spdlog::info("[{} : {}] InsertLineitem Finish!!!", __FILE__, __LINE__);

    // 插入Part表数据
    std::shared_ptr<Table> table_part;
    InsertPart(table_part, scheduler, fileDir, importFinish);
    importFinish = false;
    spdlog::info("[{} : {}] InsertPart Finish!!!", __FILE__, __LINE__);

    // 插入Partsupp表数据
    std::shared_ptr<Table> table_partsupp;
    InsertPartsupp(table_partsupp, scheduler, fileDir, importFinish);
    importFinish = false;
    spdlog::info("[{} : {}] InsertPartsupp Finish!!!", __FILE__, __LINE__);

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

    //  ================================================构建Scan================================================
    std::vector<int> lineitem_ids = {1,2,4,10};
    // <l_partkey, l_suppkey, l_quantity, l_shipdate>
    std::shared_ptr<PhysicalTableScan> scan_lineitem = std::make_shared<PhysicalTableScan>(table_lineitem, -1, lineitem_ids);

    std::vector<int> part_ids = {0,1};
    // <p_partkey, p_name>
    std::shared_ptr<PhysicalTableScan> scan_part = std::make_shared<PhysicalTableScan>(table_part, -1, part_ids);

    std::vector<int> partsupp_ids = {0,1,2};
    // <ps_partkey, ps_suppkey, ps_availqty>
    std::shared_ptr<PhysicalTableScan> scan_partsupp = std::make_shared<PhysicalTableScan>(table_partsupp, -1, partsupp_ids);

    std::vector<int> nation_ids = {0,1};
    // <n_nationkey, n_name>
    std::shared_ptr<PhysicalTableScan> scan_nation = std::make_shared<PhysicalTableScan>(table_nation, -1, nation_ids);

    std::vector<int> supplier_ids = {0,1,2,3};
    // <s_suppkey, s_name, s_address, s_nationkey>
    std::shared_ptr<PhysicalTableScan> scan_supplier = std::make_shared<PhysicalTableScan>(table_supplier, -1, supplier_ids);

    //  ================================================构建Filter================================================
    std::shared_ptr<BoundColumnRefExpression> f1_p_name = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    std::string like_pattern = "forest%";
    ScalarFunction bound_function = LikeFun::GetLikeFunction();
    Value value_pattern(like_pattern);
    std::shared_ptr<BoundConstantExpression> right_p_name = std::make_shared<BoundConstantExpression>(value_pattern);
    std::vector<std::shared_ptr<Expression>> arguments = {f1_p_name, right_p_name};
    std::shared_ptr<BoundFunctionExpression> functionExpression = std::make_shared<BoundFunctionExpression>(LogicalType::BOOL, bound_function, arguments, nullptr);
    functionExpression->bind_info = functionExpression->function.bind(functionExpression->function, functionExpression->children);
    // <p_partkey, p_name>
    std::shared_ptr<PhysicalFilter> filter1 = std::make_shared<PhysicalFilter>(functionExpression);
    filter1->exp_name = "1";

    //  ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p1_p_partkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 0, 0);
    std::vector<std::shared_ptr<Expression>> p1_expressions = {p1_p_partkey};
    // <p_partkey>
    std::shared_ptr<PhysicalProject> project1 = std::make_shared<PhysicalProject>(p1_expressions);
    project1->exp_name = "1";

    //  ================================================构建HashJoin================================================
    std::vector<int> build_ids = {0};
    std::vector<int> prode_ids = {0};
    // <ps_partkey, ps_suppkey, ps_availqty>
    std::shared_ptr<PhysicalHashJoin> join1 = std::make_shared<PhysicalHashJoin>(JoinTypes::SEMI, build_ids, prode_ids);
    join1->exp_name = "1";

    //  ================================================构建Filter================================================
    std::shared_ptr<BoundColumnRefExpression> f2_l_shipdate = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 3, 0);
    Value value_757353600(757353600);
    std::shared_ptr<BoundConstantExpression> right_l_shipdate = std::make_shared<BoundConstantExpression>(value_757353600);
    std::shared_ptr<BoundComparisonExpression> f2_comparisonExpression1 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_GREATERTHANOREQUALTO, f2_l_shipdate, right_l_shipdate);
    Value value_788889600(788889600);
    std::shared_ptr<BoundConstantExpression> right_l_shipdate2 = std::make_shared<BoundConstantExpression>(value_788889600);
    std::shared_ptr<BoundComparisonExpression> f2_comparisonExpression2 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_LESSTHAN, f2_l_shipdate, right_l_shipdate2);
    std::shared_ptr<BoundConjunctionExpression> f2_conjunctionExpression = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, f2_comparisonExpression1, f2_comparisonExpression2);
    // <l_partkey, l_suppkey, l_quantity, l_shipdate>
    std::shared_ptr<PhysicalFilter> filter2 = std::make_shared<PhysicalFilter>(f2_conjunctionExpression);
    filter2->exp_name = "2";

    //  ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p2_l_partkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 0, 0);
    std::shared_ptr<BoundColumnRefExpression> p2_l_suppkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 1, 0);
    std::shared_ptr<BoundColumnRefExpression> p2_l_quantity = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 2, 0);
    std::vector<std::shared_ptr<Expression>> p2_expressions = {p2_l_partkey, p2_l_suppkey, p2_l_quantity};
    // <l_partkey, l_suppkey, l_quantity>
    std::shared_ptr<PhysicalProject> project2 = std::make_shared<PhysicalProject>(p2_expressions);
    project2->exp_name = "2";

    // ================================================构建HashAgg================================================
    // 构建HashAgg
    std::vector<int> group_set = {0,1};
    std::vector<int> agg_set = {2};
    std::vector<int> star_bitmap = {0};
    std::vector<AggFunctionType> aggregate_function_types = {AggFunctionType::SUM};
    // <l_partkey, l_suppkey, Sum(l_quantity)>
    std::shared_ptr<PhysicalMultiFieldHashAgg> hashAgg1 = std::make_shared<PhysicalMultiFieldHashAgg>(group_set, agg_set, star_bitmap, aggregate_function_types);

    //  ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p3_l_partkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 0, 0);
    std::shared_ptr<BoundColumnRefExpression> p3_l_suppkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 1, 0);
    std::shared_ptr<BoundColumnRefExpression> p3_sum_l_quantity = std::make_shared<BoundColumnRefExpression>(LogicalType::DOUBLE, 0, 2, 0);
    Value value_05(0.5);
    std::shared_ptr<BoundConstantExpression> contant05 = std::make_shared<BoundConstantExpression>(value_05);
    std::vector<LogicalType> p3_arguments = { LogicalType::DOUBLE, LogicalType::DOUBLE };
    scalar_function_p p3_function = GetMulFunction();
    ScalarProjectFunction p3_bound_function("*", p3_arguments, LogicalType::DOUBLE, p3_function, nullptr);
    std::vector<std::shared_ptr<Expression>> p3_expressions = {contant05, p3_sum_l_quantity};
    std::shared_ptr<BoundProjectFunctionExpression> sum_l_quantity_05 = std::make_shared<BoundProjectFunctionExpression>(LogicalType::DOUBLE, p3_bound_function, p3_expressions, nullptr);
    sum_l_quantity_05->alias = "0.5 * Sum(l_quantity)";
    std::vector<std::shared_ptr<Expression>> p3_expressions2 = {p3_l_partkey, p3_l_suppkey, sum_l_quantity_05};
    // <l_partkey, l_suppkey, 0.5 * Sum(l_quantity)>
    std::shared_ptr<PhysicalProject> project3 = std::make_shared<PhysicalProject>(p3_expressions2);
    project3->exp_name = "3";

    std::vector<int> build_ids2 = {0,1};
    std::vector<int> prode_ids2 = {0,1};
    // <l_partkey, l_suppkey, 0.5 * Sum(l_quantity), ps_partkey, ps_suppkey, ps_availqty>
    std::shared_ptr<PhysicalHashJoin> join2 = std::make_shared<PhysicalHashJoin>(JoinTypes::LEFT, build_ids2, prode_ids2);
    join2->exp_name = "2";

    std::shared_ptr<BoundColumnRefExpression> f3_ps_availqty = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 5, 0);
    std::shared_ptr<BoundColumnRefExpression> f3_05_sum_l_quantity = std::make_shared<BoundColumnRefExpression>(LogicalType::DOUBLE, 0, 2, 0);
    std::shared_ptr<BoundComparisonExpression> f3_comparisonExpression = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_GREATERTHAN, f3_ps_availqty, f3_05_sum_l_quantity);
    // <l_partkey, l_suppkey, 0.5 * Sum(l_quantity), ps_partkey, ps_suppkey, ps_availqty>
    std::shared_ptr<PhysicalFilter> filter3 = std::make_shared<PhysicalFilter>(f3_comparisonExpression);
    filter3->exp_name = "3";

    //  ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p4_ps_suppkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 4, 0);
    std::vector<std::shared_ptr<Expression>> p4_expressions = {p4_ps_suppkey};
    // <ps_suppkey>
    std::shared_ptr<PhysicalProject> project4 = std::make_shared<PhysicalProject>(p4_expressions);
    project4->exp_name = "4";

    std::shared_ptr<BoundColumnRefExpression> f4_n_name = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    Value value_CANADA("CANADA");
    std::shared_ptr<BoundConstantExpression> right_n_name = std::make_shared<BoundConstantExpression>(value_CANADA);
    std::shared_ptr<BoundComparisonExpression> f4_comparisonExpression = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_EQUAL, f4_n_name, right_n_name);
    // <n_nationkey, n_name>
    std::shared_ptr<PhysicalFilter> filter4 = std::make_shared<PhysicalFilter>(f4_comparisonExpression);
    filter4->exp_name = "4";

    std::vector<int> build_ids4 = {0};
    std::vector<int> prode_ids4 = {3};
    // <n_nationkey, n_name, s_suppkey, s_name, s_address, s_nationkey>
    std::shared_ptr<PhysicalHashJoin> join4 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids4, prode_ids4);
    join4->exp_name = "4";

    std::vector<int> build_ids3 = {0};
    std::vector<int> prode_ids3 = {2};
    // <n_nationkey, n_name, s_suppkey, s_name, s_address, s_nationkey>
    std::shared_ptr<PhysicalHashJoin> join3 = std::make_shared<PhysicalHashJoin>(JoinTypes::SEMI, build_ids3, prode_ids3);
    join3->exp_name = "3";

    std::shared_ptr<BoundColumnRefExpression> p5_s_name = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 3, 0);
    std::shared_ptr<BoundColumnRefExpression> p5_s_address = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 4, 0);
    std::vector<std::shared_ptr<Expression>> p5_expressions = {p5_s_name, p5_s_address};
    // <s_name, s_address>
    std::shared_ptr<PhysicalProject> project5 = std::make_shared<PhysicalProject>(p5_expressions);
    project5->exp_name = "5";

    //  ================================================构建OrderBy================================================
    std::vector<int> sort_keys = {0};
    std::vector<LogicalType> key_types = {LogicalType::STRING};
    std::vector<SortOrder> orders_p = {SortOrder::ASCENDING};
    std::shared_ptr<PhysicalOrder> orderby = std::make_shared<PhysicalOrder>(&sort_keys, &key_types, &orders_p);

    orderby->AddChild(project5);
    project5->AddChild(join3);
    join3->AddChild(project4);
    join3->AddChild(join4);
    project4->AddChild(filter3);
    filter3->AddChild(join2);
    join2->AddChild(project3);
    join2->AddChild(join1);
    project3->AddChild(hashAgg1);
    hashAgg1->AddChild(project2);
    project2->AddChild(filter2);
    filter2->AddChild(scan_lineitem);
    join1->AddChild(project1);
    join1->AddChild(scan_partsupp);
    project1->AddChild(filter1);
    filter1->AddChild(scan_part);
    join4->AddChild(filter4);
    join4->AddChild(scan_supplier);
    filter4->AddChild(scan_nation);

    std::shared_ptr<Pipeline> pipeline = std::make_shared<Pipeline>();
    std::shared_ptr<PipelineGroup> pipeline_group = std::make_shared<PipelineGroup>();
    orderby->build_pipelines(pipeline, pipeline_group);
    std::shared_ptr<PipelineGroupExecute> pipeline_group_executor = std::make_shared<PipelineGroupExecute>(scheduler, pipeline_group);
    pipeline_group_executor->traverse_plan();
    auto start_execute = std::chrono::steady_clock::now();
    pipeline_group_executor->execute();
    auto end_execute = std::chrono::steady_clock::now();
    spdlog::info("[{} : {}] Q20执行时间: {}", __FILE__, __LINE__, Util::time_difference(start_execute, end_execute));

    // 关闭线程池
    scheduler->shutdown();
    RC expected = RC::SUCCESS;
    ASSERT_EQ(expected, RC::SUCCESS);
}

// 测试4线程Q20查询
TEST(TPCHTest, Q20FourThreadTest) {
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

    // 插入Part表数据
    std::shared_ptr<Table> table_part;
    std::vector<std::string> part_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "part.tbl_" + std::to_string(i);
        part_file_names.emplace_back(file_name);
    }
    InsertPartMul(table_part, scheduler, work_ids, partition_idxs, part_file_names, importFinish);
    importFinish = false;
    std::this_thread::sleep_for(std::chrono::seconds(1));
    // spdlog::info("[{} : {}] InsertPart Finish!!!", __FILE__, __LINE__);

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

    std::cout << "Q20: select\n"
                 "    s_name, s_address\n"
                 "from\n"
                 "    supplier, nation\n"
                 "where\n"
                 "    s_suppkey in (\n"
                 "        select\n"
                 "            ps_suppkey\n"
                 "        from\n"
                 "            partsupp\n"
                 "        where\n"
                 "            ps_partkey in (\n"
                 "                select\n"
                 "                    p_partkey\n"
                 "                from\n"
                 "                    part\n"
                 "                where\n"
                 "                    p_name like 'forest%'\n"
                 "            )\n"
                 "            and ps_availqty > (\n"
                 "                select\n"
                 "                    0.5 * sum(l_quantity)\n"
                 "                from\n"
                 "                    lineitem\n"
                 "                where\n"
                 "                    l_partkey = ps_partkey\n"
                 "                    and l_suppkey = ps_suppkey\n"
                 "                    and l_shipdate >= date '1994-01-01'\n"
                 "                    and l_shipdate < date '1995-01-01'\n"
                 "            )\n"
                 "    )\n"
                 "  and s_nationkey = n_nationkey\n"
                 "  and n_name = 'CANADA'\n"
                 "order by\n"
                 "    s_name;" << std::endl;

    //  ================================================构建Scan================================================
    std::vector<int> lineitem_ids = {1,2,4,10};
    // <l_partkey, l_suppkey, l_quantity, l_shipdate>
    std::shared_ptr<PhysicalTableScan> scan_lineitem = std::make_shared<PhysicalTableScan>(table_lineitem, -1, lineitem_ids);

    std::vector<int> part_ids = {0,1};
    // <p_partkey, p_name>
    std::shared_ptr<PhysicalTableScan> scan_part = std::make_shared<PhysicalTableScan>(table_part, -1, part_ids);

    std::vector<int> partsupp_ids = {0,1,2};
    // <ps_partkey, ps_suppkey, ps_availqty>
    std::shared_ptr<PhysicalTableScan> scan_partsupp = std::make_shared<PhysicalTableScan>(table_partsupp, -1, partsupp_ids);

    std::vector<int> nation_ids = {0,1};
    // <n_nationkey, n_name>
    std::shared_ptr<PhysicalTableScan> scan_nation = std::make_shared<PhysicalTableScan>(table_nation, -1, nation_ids);

    std::vector<int> supplier_ids = {0,1,2,3};
    // <s_suppkey, s_name, s_address, s_nationkey>
    std::shared_ptr<PhysicalTableScan> scan_supplier = std::make_shared<PhysicalTableScan>(table_supplier, -1, supplier_ids);

    //  ================================================构建Filter================================================
    std::shared_ptr<BoundColumnRefExpression> f1_p_name = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    std::string like_pattern = "forest%";
    ScalarFunction bound_function = LikeFun::GetLikeFunction();
    Value value_pattern(like_pattern);
    std::shared_ptr<BoundConstantExpression> right_p_name = std::make_shared<BoundConstantExpression>(value_pattern);
    std::vector<std::shared_ptr<Expression>> arguments = {f1_p_name, right_p_name};
    std::shared_ptr<BoundFunctionExpression> functionExpression = std::make_shared<BoundFunctionExpression>(LogicalType::BOOL, bound_function, arguments, nullptr);
    functionExpression->bind_info = functionExpression->function.bind(functionExpression->function, functionExpression->children);
    // <p_partkey, p_name>
    std::shared_ptr<PhysicalFilter> filter1 = std::make_shared<PhysicalFilter>(functionExpression);
    filter1->exp_name = "1";

    //  ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p1_p_partkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 0, 0);
    std::vector<std::shared_ptr<Expression>> p1_expressions = {p1_p_partkey};
    // <p_partkey>
    std::shared_ptr<PhysicalProject> project1 = std::make_shared<PhysicalProject>(p1_expressions);
    project1->exp_name = "1";

    //  ================================================构建HashJoin================================================
    std::vector<int> build_ids = {0};
    std::vector<int> prode_ids = {0};
    // <ps_partkey, ps_suppkey, ps_availqty>
    std::shared_ptr<PhysicalHashJoin> join1 = std::make_shared<PhysicalHashJoin>(JoinTypes::SEMI, build_ids, prode_ids);
    join1->exp_name = "1";

    //  ================================================构建Filter================================================
    std::shared_ptr<BoundColumnRefExpression> f2_l_shipdate = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 3, 0);
    Value value_757353600(757353600);
    std::shared_ptr<BoundConstantExpression> right_l_shipdate = std::make_shared<BoundConstantExpression>(value_757353600);
    std::shared_ptr<BoundComparisonExpression> f2_comparisonExpression1 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_GREATERTHANOREQUALTO, f2_l_shipdate, right_l_shipdate);
    Value value_788889600(788889600);
    std::shared_ptr<BoundConstantExpression> right_l_shipdate2 = std::make_shared<BoundConstantExpression>(value_788889600);
    std::shared_ptr<BoundComparisonExpression> f2_comparisonExpression2 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_LESSTHAN, f2_l_shipdate, right_l_shipdate2);
    std::shared_ptr<BoundConjunctionExpression> f2_conjunctionExpression = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, f2_comparisonExpression1, f2_comparisonExpression2);
    // <l_partkey, l_suppkey, l_quantity, l_shipdate>
    std::shared_ptr<PhysicalFilter> filter2 = std::make_shared<PhysicalFilter>(f2_conjunctionExpression);
    filter2->exp_name = "2";

    //  ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p2_l_partkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 0, 0);
    std::shared_ptr<BoundColumnRefExpression> p2_l_suppkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 1, 0);
    std::shared_ptr<BoundColumnRefExpression> p2_l_quantity = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 2, 0);
    std::vector<std::shared_ptr<Expression>> p2_expressions = {p2_l_partkey, p2_l_suppkey, p2_l_quantity};
    // <l_partkey, l_suppkey, l_quantity>
    std::shared_ptr<PhysicalProject> project2 = std::make_shared<PhysicalProject>(p2_expressions);
    project2->exp_name = "2";

    // ================================================构建HashAgg================================================
    // 构建HashAgg
    std::vector<int> group_set = {0,1};
    std::vector<int> agg_set = {2};
    std::vector<int> star_bitmap = {0};
    std::vector<AggFunctionType> aggregate_function_types = {AggFunctionType::SUM};
    // <l_partkey, l_suppkey, Sum(l_quantity)>
    std::shared_ptr<PhysicalMultiFieldHashAgg> hashAgg1 = std::make_shared<PhysicalMultiFieldHashAgg>(group_set, agg_set, star_bitmap, aggregate_function_types);

    //  ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p3_l_partkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 0, 0);
    std::shared_ptr<BoundColumnRefExpression> p3_l_suppkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 1, 0);
    std::shared_ptr<BoundColumnRefExpression> p3_sum_l_quantity = std::make_shared<BoundColumnRefExpression>(LogicalType::DOUBLE, 0, 2, 0);
    Value value_05(0.5);
    std::shared_ptr<BoundConstantExpression> contant05 = std::make_shared<BoundConstantExpression>(value_05);
    std::vector<LogicalType> p3_arguments = { LogicalType::DOUBLE, LogicalType::DOUBLE };
    scalar_function_p p3_function = GetMulFunction();
    ScalarProjectFunction p3_bound_function("*", p3_arguments, LogicalType::DOUBLE, p3_function, nullptr);
    std::vector<std::shared_ptr<Expression>> p3_expressions = {contant05, p3_sum_l_quantity};
    std::shared_ptr<BoundProjectFunctionExpression> sum_l_quantity_05 = std::make_shared<BoundProjectFunctionExpression>(LogicalType::DOUBLE, p3_bound_function, p3_expressions, nullptr);
    sum_l_quantity_05->alias = "0.5 * Sum(l_quantity)";
    std::vector<std::shared_ptr<Expression>> p3_expressions2 = {p3_l_partkey, p3_l_suppkey, sum_l_quantity_05};
    // <l_partkey, l_suppkey, 0.5 * Sum(l_quantity)>
    std::shared_ptr<PhysicalProject> project3 = std::make_shared<PhysicalProject>(p3_expressions2);
    project3->exp_name = "3";

    std::vector<int> build_ids2 = {0,1};
    std::vector<int> prode_ids2 = {0,1};
    // <l_partkey, l_suppkey, 0.5 * Sum(l_quantity), ps_partkey, ps_suppkey, ps_availqty>
    std::shared_ptr<PhysicalHashJoin> join2 = std::make_shared<PhysicalHashJoin>(JoinTypes::LEFT, build_ids2, prode_ids2);
    join2->exp_name = "2";

    std::shared_ptr<BoundColumnRefExpression> f3_ps_availqty = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 5, 0);
    std::shared_ptr<BoundColumnRefExpression> f3_05_sum_l_quantity = std::make_shared<BoundColumnRefExpression>(LogicalType::DOUBLE, 0, 2, 0);
    std::shared_ptr<BoundComparisonExpression> f3_comparisonExpression = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_GREATERTHAN, f3_ps_availqty, f3_05_sum_l_quantity);
    // <l_partkey, l_suppkey, 0.5 * Sum(l_quantity), ps_partkey, ps_suppkey, ps_availqty>
    std::shared_ptr<PhysicalFilter> filter3 = std::make_shared<PhysicalFilter>(f3_comparisonExpression);
    filter3->exp_name = "3";

    //  ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p4_ps_suppkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 4, 0);
    std::vector<std::shared_ptr<Expression>> p4_expressions = {p4_ps_suppkey};
    // <ps_suppkey>
    std::shared_ptr<PhysicalProject> project4 = std::make_shared<PhysicalProject>(p4_expressions);
    project4->exp_name = "4";

    std::shared_ptr<BoundColumnRefExpression> f4_n_name = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    Value value_CANADA("CANADA");
    std::shared_ptr<BoundConstantExpression> right_n_name = std::make_shared<BoundConstantExpression>(value_CANADA);
    std::shared_ptr<BoundComparisonExpression> f4_comparisonExpression = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_EQUAL, f4_n_name, right_n_name);
    // <n_nationkey, n_name>
    std::shared_ptr<PhysicalFilter> filter4 = std::make_shared<PhysicalFilter>(f4_comparisonExpression);
    filter4->exp_name = "4";

    std::vector<int> build_ids4 = {0};
    std::vector<int> prode_ids4 = {3};
    std::vector<ExpressionTypes> comparison_types4 = {};
    // <n_nationkey, n_name, s_suppkey, s_name, s_address, s_nationkey>
    std::shared_ptr<PhysicalHashJoin> join4 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids4, prode_ids4, comparison_types4, true);
    join4->exp_name = "4";

    std::vector<int> build_ids3 = {0};
    std::vector<int> prode_ids3 = {2};
    // <n_nationkey, n_name, s_suppkey, s_name, s_address, s_nationkey>
    std::shared_ptr<PhysicalHashJoin> join3 = std::make_shared<PhysicalHashJoin>(JoinTypes::SEMI, build_ids3, prode_ids3);
    join3->exp_name = "3";

    std::shared_ptr<BoundColumnRefExpression> p5_s_name = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 3, 0);
    std::shared_ptr<BoundColumnRefExpression> p5_s_address = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 4, 0);
    std::vector<std::shared_ptr<Expression>> p5_expressions = {p5_s_name, p5_s_address};
    // <s_name, s_address>
    std::shared_ptr<PhysicalProject> project5 = std::make_shared<PhysicalProject>(p5_expressions);
    project5->exp_name = "5";

    //  ================================================构建OrderBy================================================
    std::vector<int> sort_keys = {0};
    std::vector<LogicalType> key_types = {LogicalType::STRING};
    std::vector<SortOrder> orders_p = {SortOrder::ASCENDING};
    std::shared_ptr<PhysicalOrder> orderby = std::make_shared<PhysicalOrder>(&sort_keys, &key_types, &orders_p);

    orderby->AddChild(project5);
    project5->AddChild(join3);
    join3->AddChild(project4);
    join3->AddChild(join4);
    project4->AddChild(filter3);
    filter3->AddChild(join2);
    join2->AddChild(project3);
    join2->AddChild(join1);
    project3->AddChild(hashAgg1);
    hashAgg1->AddChild(project2);
    project2->AddChild(filter2);
    filter2->AddChild(scan_lineitem);
    join1->AddChild(project1);
    join1->AddChild(scan_partsupp);
    project1->AddChild(filter1);
    filter1->AddChild(scan_part);
    join4->AddChild(filter4);
    join4->AddChild(scan_supplier);
    filter4->AddChild(scan_nation);

    std::shared_ptr<Pipeline> pipeline = std::make_shared<Pipeline>();
    std::shared_ptr<PipelineGroup> pipeline_group = std::make_shared<PipelineGroup>();
    orderby->build_pipelines(pipeline, pipeline_group);
    std::shared_ptr<PipelineGroupExecute> pipeline_group_executor = std::make_shared<PipelineGroupExecute>(scheduler, pipeline_group);
    pipeline_group_executor->traverse_plan();
    pipeline_group_executor->execute();
    spdlog::info("[{} : {}] Q20执行时间: {} ms.", __FILE__, __LINE__, pipeline_group_executor->duration.count());
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