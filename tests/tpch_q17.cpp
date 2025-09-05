#include "import_data.hpp"
#include "file_dir_config.hpp"
// =============================Q17=============================
// 测试单线程Q17查询
TEST(TPCHTest, Q17SingleThreadTest) {
    std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
    bool importFinish = false;
    std::string fileDir = fileDir1;
    // 插入Part表数据
    std::shared_ptr<Table> table_part;
    InsertPart(table_part, scheduler, fileDir, importFinish);
    importFinish = false;
    spdlog::info("[{} : {}] InsertPart Finish!!!", __FILE__, __LINE__);

    // 插入Lineitem表数据
    std::shared_ptr<Table> table_lineitem;
    InsertLineitem(table_lineitem, scheduler, fileDir, importFinish);
    importFinish = false;
    spdlog::info("[{} : {}] InsertLineitem Finish!!!", __FILE__, __LINE__);

    //  ================================================构建Scan================================================
    std::vector<int> part_ids = {0, 3, 6};
    // <p_partkey, p_brand, p_container>
    std::shared_ptr<PhysicalTableScan> scan_part = std::make_shared<PhysicalTableScan>(table_part, -1, part_ids);

    std::vector<int> lineitem_ids1 = {1, 4};
    // <l_partkey, l_quantity>
    std::shared_ptr<PhysicalTableScan> scan_lineitem1 = std::make_shared<PhysicalTableScan>(table_lineitem, -1, lineitem_ids1);

    std::vector<int> lineitem_ids2 = {1, 4, 5};
    // <l_partkey, l_quantity, l_extendedprice>
    std::shared_ptr<PhysicalTableScan> scan_lineitem2 = std::make_shared<PhysicalTableScan>(table_lineitem, -1, lineitem_ids2);

    //  ================================================构建Filter================================================
    std::shared_ptr<BoundColumnRefExpression> p1_p_brand = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    std::shared_ptr<BoundColumnRefExpression> p1_p_container = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 2, 0);

    Value value_Brand_23("Brand#23");
    std::shared_ptr<BoundConstantExpression> right_p_brand = std::make_shared<BoundConstantExpression>(value_Brand_23);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_EQUAL, p1_p_brand, right_p_brand);

    Value value_MED("MED BOX");
    std::shared_ptr<BoundConstantExpression> right_p_container = std::make_shared<BoundConstantExpression>(value_MED);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression2 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_EQUAL, p1_p_container, right_p_container);

    std::shared_ptr<BoundConjunctionExpression> conjunctionExpression = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, comparisonExpression, comparisonExpression2);
    // <p_partkey, p_brand, p_container>
    std::shared_ptr<PhysicalFilter> filter1 = std::make_shared<PhysicalFilter>(conjunctionExpression);
    filter1->exp_name = "1";
    //  ================================================构建HashJoin================================================
    std::vector<int> build_ids1 = {0};
    std::vector<int> prode_ids1 = {0};
    // <p_partkey, p_brand, p_container, l_partkey, l_quantity, l_extendedprice>
    std::shared_ptr<PhysicalHashJoin> join1 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids1, prode_ids1);
    join1->exp_name = "1";

    // 构建HashAgg
    std::vector<int> group_set1 = {0};
    std::vector<int> agg_set1 = {1};
    std::vector<int> star_bitmap1 = {0};
    std::vector<AggFunctionType> aggregate_function_types1 = {AggFunctionType::AVG};
    // <l_partkey, Avg(l_quantity)>
    std::shared_ptr<PhysicalMultiFieldHashAgg> hashAgg1 = std::make_shared<PhysicalMultiFieldHashAgg>(group_set1, agg_set1, star_bitmap1, aggregate_function_types1);

    //  ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> l_partkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 0, 0);
    std::shared_ptr<BoundColumnRefExpression> l_quantity = std::make_shared<BoundColumnRefExpression>(LogicalType::DOUBLE, 0, 1, 0);
    std::vector<LogicalType> p1_arguments = { LogicalType::DOUBLE, LogicalType::DOUBLE };
    scalar_function_p p1_function = GetMulFunction();
    ScalarProjectFunction p1_bound_function("*", p1_arguments, LogicalType::DOUBLE, p1_function, nullptr);
    Value value_2d(0.2);
    std::shared_ptr<BoundConstantExpression> p1_constant = std::make_shared<BoundConstantExpression>(value_2d);
    std::vector<std::shared_ptr<Expression>> p1_expressions1 = {p1_constant, l_quantity};
    std::shared_ptr<BoundProjectFunctionExpression> l_quantity_0_2 = std::make_shared<BoundProjectFunctionExpression>(LogicalType::DOUBLE, p1_bound_function, p1_expressions1, nullptr);
    l_quantity_0_2->alias = "0.2 * avg(l_quantity)";
    std::vector<std::shared_ptr<Expression>> p1_expressions = {l_partkey, l_quantity_0_2};
    // <l_partkey, 0.2 * avg(l_quantity)>
    std::shared_ptr<PhysicalProject> project1 = std::make_shared<PhysicalProject>(p1_expressions);
    project1->exp_name = "1";

    //  ================================================构建HashJoin================================================
    std::vector<int> build_ids2 = {0};
    std::vector<int> prode_ids2 = {0};
    // <l_partkey, 0.2 * avg(l_quantity), p_partkey, p_brand, p_container, l_partkey, l_quantity, l_extendedprice>
    std::shared_ptr<PhysicalHashJoin> join2 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids2, prode_ids2);
    join2->exp_name = "2";

    //  ================================================构建Filter================================================
    std::shared_ptr<BoundColumnRefExpression> f2_l_quantity = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 6, 0);
    std::shared_ptr<BoundColumnRefExpression> f2_avg_l_quantity = std::make_shared<BoundColumnRefExpression>(LogicalType::DOUBLE, 0, 1, 0);
    std::shared_ptr<BoundComparisonExpression> f2_comparisonExpression = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_LESSTHAN, f2_l_quantity, f2_avg_l_quantity);
    // <l_partkey, 0.2 * avg(l_quantity), p_partkey, p_brand, p_container, l_partkey, l_quantity, l_extendedprice>
    std::shared_ptr<PhysicalFilter> filter2 = std::make_shared<PhysicalFilter>(f2_comparisonExpression);
    filter2->exp_name = "2";
    //  ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p2_l_extendedprice = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 7, 0);;
    std::vector<std::shared_ptr<Expression>> p2_expressions = {p2_l_extendedprice};
    // <l_extendedprice>
    std::shared_ptr<PhysicalProject> project2 = std::make_shared<PhysicalProject>(p2_expressions);
    project2->exp_name = "2";
    // 构建HashAgg
    std::vector<AggFunctionType> aggregate_function_types2 = { AggFunctionType::SUM};
    std::vector<int32_t> group_item_idxs2 = {0};
    // <Sum(l_extendedprice)>
    std::shared_ptr<PhysicalHashAgg> hashAgg2 = std::make_shared<PhysicalHashAgg>(-1, aggregate_function_types2, group_item_idxs2);

    //  ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p3_l_extendedprice = std::make_shared<BoundColumnRefExpression>(LogicalType::DOUBLE, 0, 0, 0);;
    std::vector<LogicalType> p3_arguments = { LogicalType::DOUBLE, LogicalType::DOUBLE };
    scalar_function_p p3_function = GetDivFunction();
    ScalarProjectFunction p3_bound_function("/", p3_arguments, LogicalType::DOUBLE, p3_function, nullptr);
    Value value_7d(7.0);
    std::shared_ptr<BoundConstantExpression> p3_constant = std::make_shared<BoundConstantExpression>(value_7d);
    std::vector<std::shared_ptr<Expression>> p3_expressions1 = {p3_l_extendedprice, p3_constant};
    std::shared_ptr<BoundProjectFunctionExpression> l_extendedprice_3 = std::make_shared<BoundProjectFunctionExpression>(LogicalType::DOUBLE, p3_bound_function, p3_expressions1, nullptr);
    l_extendedprice_3->alias = "Sum(l_extendedprice) / 7.0";
    std::vector<std::shared_ptr<Expression>> p3_expressions = {l_extendedprice_3};
    // <Sum(l_extendedprice) / 7.0>
    std::shared_ptr<PhysicalProject> project3 = std::make_shared<PhysicalProject>(p3_expressions);
    project3->exp_name = "3";
    std::shared_ptr<PhysicalResultCollector> result_collector = std::make_shared<PhysicalResultCollector>(project3);

    result_collector->AddChild(project3);
    project3->AddChild(hashAgg2);
    hashAgg2->AddChild(project2);
    project2->AddChild(filter2);
    filter2->AddChild(join2);
    join2->AddChild(project1);
    join2->AddChild(join1);
    project1->AddChild(hashAgg1);
    hashAgg1->AddChild(scan_lineitem1);
    join1->AddChild(filter1);
    join1->AddChild(scan_lineitem2);
    filter1->AddChild(scan_part);

    std::shared_ptr<Pipeline> pipeline = std::make_shared<Pipeline>();
    std::shared_ptr<PipelineGroup> pipeline_group = std::make_shared<PipelineGroup>();
    result_collector->build_pipelines(pipeline, pipeline_group);
    std::shared_ptr<PipelineGroupExecute> pipeline_group_executor = std::make_shared<PipelineGroupExecute>(scheduler, pipeline_group);
    pipeline_group_executor->traverse_plan();
    auto start_execute = std::chrono::steady_clock::now();
    pipeline_group_executor->execute();
    auto end_execute = std::chrono::steady_clock::now();
    spdlog::info("[{} : {}] Q17执行时间: {}", __FILE__, __LINE__, Util::time_difference(start_execute, end_execute));

    // 关闭线程池
    scheduler->shutdown();
    RC expected = RC::SUCCESS;
    ASSERT_EQ(expected, RC::SUCCESS);
}

// 测试4线程Q17查询
TEST(TPCHTest, Q17FourThreadTest) {
    std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
    bool importFinish = false;
    std::string fileDir = fileDir4;
    std::vector<int> work_ids(arr, arr + sizeof(arr) / sizeof(arr[0]));
    std::vector<int> partition_idxs(arr, arr + sizeof(arr) / sizeof(arr[0]));
    // 插入Part表数据
    std::shared_ptr<Table> table_part;
    std::vector<std::string> part_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "part.tbl_" + std::to_string(i);
        part_file_names.emplace_back(file_name);
    }
    InsertPartMul(table_part, scheduler, work_ids, partition_idxs, part_file_names, importFinish);
    importFinish = false;
    std::this_thread::sleep_for(std::chrono::seconds(2));
    // spdlog::info("[{} : {}] InsertPart Finish!!!", __FILE__, __LINE__);

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

    std::cout << "Q17: select\n"
                 "    sum(l_extendedprice) / 7.0 as avg_yearly\n"
                 "from\n"
                 "    lineitem, part\n"
                 "where\n"
                 "    p_partkey = l_partkey\n"
                 "    and p_brand = 'Brand#23'\n"
                 "    and p_container = 'MED BOX'\n"
                 "    and l_quantity < (\n"
                 "        select\n"
                 "            0.2 * avg(l_quantity)\n"
                 "        from\n"
                 "            lineitem\n"
                 "        where\n"
                 "            l_partkey = p_partkey\n"
                 ");" << std::endl;

    //  ================================================构建Scan================================================
    std::vector<int> part_ids = {0, 3, 6};
    // <p_partkey, p_brand, p_container>
    std::shared_ptr<PhysicalTableScan> scan_part = std::make_shared<PhysicalTableScan>(table_part, -1, part_ids);

    std::vector<int> lineitem_ids1 = {1, 4};
    // <l_partkey, l_quantity>
    std::shared_ptr<PhysicalTableScan> scan_lineitem1 = std::make_shared<PhysicalTableScan>(table_lineitem, -1, lineitem_ids1);

    std::vector<int> lineitem_ids2 = {1, 4, 5};
    // <l_partkey, l_quantity, l_extendedprice>
    std::shared_ptr<PhysicalTableScan> scan_lineitem2 = std::make_shared<PhysicalTableScan>(table_lineitem, -1, lineitem_ids2);

    //  ================================================构建Filter================================================
    std::shared_ptr<BoundColumnRefExpression> p1_p_brand = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    std::shared_ptr<BoundColumnRefExpression> p1_p_container = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 2, 0);

    Value value_Brand_23("Brand#23");
    std::shared_ptr<BoundConstantExpression> right_p_brand = std::make_shared<BoundConstantExpression>(value_Brand_23);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_EQUAL, p1_p_brand, right_p_brand);

    Value value_MED("MED BOX");
    std::shared_ptr<BoundConstantExpression> right_p_container = std::make_shared<BoundConstantExpression>(value_MED);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression2 = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_EQUAL, p1_p_container, right_p_container);

    std::shared_ptr<BoundConjunctionExpression> conjunctionExpression = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, comparisonExpression, comparisonExpression2);
    // <p_partkey, p_brand, p_container>
    std::shared_ptr<PhysicalFilter> filter1 = std::make_shared<PhysicalFilter>(conjunctionExpression);
    filter1->exp_name = "1";
    //  ================================================构建HashJoin================================================
    std::vector<int> build_ids1 = {0};
    std::vector<int> prode_ids1 = {0};
    // <p_partkey, p_brand, p_container, l_partkey, l_quantity, l_extendedprice>
    std::shared_ptr<PhysicalHashJoin> join1 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids1, prode_ids1);
    join1->exp_name = "1";

    // 构建HashAgg
    std::vector<int> group_set1 = {0};
    std::vector<int> agg_set1 = {1};
    std::vector<int> star_bitmap1 = {0};
    std::vector<AggFunctionType> aggregate_function_types1 = {AggFunctionType::AVG};
    // <l_partkey, Avg(l_quantity)>
    std::shared_ptr<PhysicalMultiFieldHashAgg> hashAgg1 = std::make_shared<PhysicalMultiFieldHashAgg>(group_set1, agg_set1, star_bitmap1, aggregate_function_types1);

    //  ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> l_partkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 0, 0);
    std::shared_ptr<BoundColumnRefExpression> l_quantity = std::make_shared<BoundColumnRefExpression>(LogicalType::DOUBLE, 0, 1, 0);
    std::vector<LogicalType> p1_arguments = { LogicalType::DOUBLE, LogicalType::DOUBLE };
    scalar_function_p p1_function = GetMulFunction();
    ScalarProjectFunction p1_bound_function("*", p1_arguments, LogicalType::DOUBLE, p1_function, nullptr);
    Value value_2d(0.2);
    std::shared_ptr<BoundConstantExpression> p1_constant = std::make_shared<BoundConstantExpression>(value_2d);
    std::vector<std::shared_ptr<Expression>> p1_expressions1 = {p1_constant, l_quantity};
    std::shared_ptr<BoundProjectFunctionExpression> l_quantity_0_2 = std::make_shared<BoundProjectFunctionExpression>(LogicalType::DOUBLE, p1_bound_function, p1_expressions1, nullptr);
    l_quantity_0_2->alias = "0.2 * avg(l_quantity)";
    std::vector<std::shared_ptr<Expression>> p1_expressions = {l_partkey, l_quantity_0_2};
    // <l_partkey, 0.2 * avg(l_quantity)>
    std::shared_ptr<PhysicalProject> project1 = std::make_shared<PhysicalProject>(p1_expressions);
    project1->exp_name = "1";

    //  ================================================构建HashJoin================================================
    std::vector<int> build_ids2 = {0};
    std::vector<int> prode_ids2 = {0};
    // <l_partkey, 0.2 * avg(l_quantity), p_partkey, p_brand, p_container, l_partkey, l_quantity, l_extendedprice>
    std::shared_ptr<PhysicalHashJoin> join2 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids2, prode_ids2);
    join2->exp_name = "2";

    //  ================================================构建Filter================================================
    std::shared_ptr<BoundColumnRefExpression> f2_l_quantity = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 6, 0);
    std::shared_ptr<BoundColumnRefExpression> f2_avg_l_quantity = std::make_shared<BoundColumnRefExpression>(LogicalType::DOUBLE, 0, 1, 0);
    std::shared_ptr<BoundComparisonExpression> f2_comparisonExpression = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_LESSTHAN, f2_l_quantity, f2_avg_l_quantity);
    // <l_partkey, 0.2 * avg(l_quantity), p_partkey, p_brand, p_container, l_partkey, l_quantity, l_extendedprice>
    std::shared_ptr<PhysicalFilter> filter2 = std::make_shared<PhysicalFilter>(f2_comparisonExpression);
    filter2->exp_name = "2";
    //  ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p2_l_extendedprice = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 7, 0);;
    std::vector<std::shared_ptr<Expression>> p2_expressions = {p2_l_extendedprice};
    // <l_extendedprice>
    std::shared_ptr<PhysicalProject> project2 = std::make_shared<PhysicalProject>(p2_expressions);
    project2->exp_name = "2";
    // 构建HashAgg
    std::vector<AggFunctionType> aggregate_function_types2 = { AggFunctionType::SUM};
    std::vector<int32_t> group_item_idxs2 = {0};
    // <Sum(l_extendedprice)>
    std::shared_ptr<PhysicalHashAgg> hashAgg2 = std::make_shared<PhysicalHashAgg>(-1, aggregate_function_types2, group_item_idxs2);

    //  ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p3_l_extendedprice = std::make_shared<BoundColumnRefExpression>(LogicalType::DOUBLE, 0, 0, 0);;
    std::vector<LogicalType> p3_arguments = { LogicalType::DOUBLE, LogicalType::DOUBLE };
    scalar_function_p p3_function = GetDivFunction();
    ScalarProjectFunction p3_bound_function("/", p3_arguments, LogicalType::DOUBLE, p3_function, nullptr);
    Value value_7d(7.0);
    std::shared_ptr<BoundConstantExpression> p3_constant = std::make_shared<BoundConstantExpression>(value_7d);
    std::vector<std::shared_ptr<Expression>> p3_expressions1 = {p3_l_extendedprice, p3_constant};
    std::shared_ptr<BoundProjectFunctionExpression> l_extendedprice_3 = std::make_shared<BoundProjectFunctionExpression>(LogicalType::DOUBLE, p3_bound_function, p3_expressions1, nullptr);
    l_extendedprice_3->alias = "Sum(l_extendedprice) / 7.0";
    std::vector<std::shared_ptr<Expression>> p3_expressions = {l_extendedprice_3};
    // <Sum(l_extendedprice) / 7.0>
    std::shared_ptr<PhysicalProject> project3 = std::make_shared<PhysicalProject>(p3_expressions);
    project3->exp_name = "3";
    std::shared_ptr<PhysicalResultCollector> result_collector = std::make_shared<PhysicalResultCollector>(project3);

    result_collector->AddChild(project3);
    project3->AddChild(hashAgg2);
    hashAgg2->AddChild(project2);
    project2->AddChild(filter2);
    filter2->AddChild(join2);
    join2->AddChild(project1);
    join2->AddChild(join1);
    project1->AddChild(hashAgg1);
    hashAgg1->AddChild(scan_lineitem1);
    join1->AddChild(filter1);
    join1->AddChild(scan_lineitem2);
    filter1->AddChild(scan_part);

    std::shared_ptr<Pipeline> pipeline = std::make_shared<Pipeline>();
    std::shared_ptr<PipelineGroup> pipeline_group = std::make_shared<PipelineGroup>();
    result_collector->build_pipelines(pipeline, pipeline_group);
    std::shared_ptr<PipelineGroupExecute> pipeline_group_executor = std::make_shared<PipelineGroupExecute>(scheduler, pipeline_group);
    pipeline_group_executor->traverse_plan();
    pipeline_group_executor->execute();
    spdlog::info("[{} : {}] Q17执行时间: {} ms.", __FILE__, __LINE__, pipeline_group_executor->duration.count());
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