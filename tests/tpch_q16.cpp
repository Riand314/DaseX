#include "import_data.hpp"
#include "file_dir_config.hpp"
// =============================Q16=============================
// 测试单线程Q16查询
TEST(TPCHTest, Q16SingleThreadTest) {
    std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
    bool importFinish = false;
    std::string fileDir = fileDir1;
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

    // 插入Supplier表数据
    std::shared_ptr<Table> table_supplier;
    InsertSupplier(table_supplier, scheduler, fileDir, importFinish);
    importFinish = false;
    spdlog::info("[{} : {}] InsertSupplier Finish!!!", __FILE__, __LINE__);

    //  ================================================构建Scan================================================
    std::vector<int> part_ids = {0, 3, 4, 5};
    // <p_partkey, p_brand, p_type, p_size>
    std::shared_ptr<PhysicalTableScan> scan_part = std::make_shared<PhysicalTableScan>(table_part, -1, part_ids);

    std::vector<int> partsupp_ids = {0, 1};
    // <ps_partkey, ps_suppkey>
    std::shared_ptr<PhysicalTableScan> scan_partsupp = std::make_shared<PhysicalTableScan>(table_partsupp, -1, partsupp_ids);

    std::vector<int> supplier_ids = {0, 6};
    // <s_suppkey, s_comment>
    std::shared_ptr<PhysicalTableScan> scan_supplier = std::make_shared<PhysicalTableScan>(table_supplier, -1, supplier_ids);

    //  ================================================构建Filter================================================
    std::shared_ptr<BoundColumnRefExpression> p1_p_brand = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    std::shared_ptr<BoundColumnRefExpression> p1_p_type = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 2, 0);
    std::shared_ptr<BoundColumnRefExpression> p1_p_size = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 3, 0);
    Value value_Brand_45("Brand#45");
    std::shared_ptr<BoundConstantExpression> right_p_brand = std::make_shared<BoundConstantExpression>(value_Brand_45);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_NOTEQUAL, p1_p_brand, right_p_brand);

    std::string like_pattern = "MEDIUM POLISHED%";
    ScalarFunction bound_function = NotLikeFun::GetNotLikeFunction();
    Value value_pattern(like_pattern);
    std::shared_ptr<BoundConstantExpression> right_p_type = std::make_shared<BoundConstantExpression>(value_pattern);
    std::vector<std::shared_ptr<Expression>> arguments;
    arguments.push_back(std::move(p1_p_type));
    arguments.push_back(std::move(right_p_type));
    std::shared_ptr<BoundFunctionExpression> functionExpression = std::make_shared<BoundFunctionExpression>(LogicalType::BOOL, bound_function, arguments, nullptr);
    functionExpression->bind_info = functionExpression->function.bind(functionExpression->function, functionExpression->children);

    std::vector<Value> value_set = {Value(49), Value(14), Value(23), Value(45), Value(19), Value(3), Value(36), Value(9)};
    std::shared_ptr<BoundInExpression> inExpression = std::make_shared<BoundInExpression>(p1_p_size, value_set, LogicalType::INTEGER);

    std::shared_ptr<BoundConjunctionExpression> conjunctionExpression1 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, comparisonExpression, functionExpression);
    std::shared_ptr<BoundConjunctionExpression> conjunctionExpression2 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, conjunctionExpression1, inExpression);
    // <p_partkey, p_brand, p_type, p_size>
    std::shared_ptr<PhysicalFilter> filter1 = std::make_shared<PhysicalFilter>(conjunctionExpression2);

    std::shared_ptr<BoundColumnRefExpression> p2_s_comment = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    std::string like_pattern2 = "%Customer%Complaints%";
    ScalarFunction bound_function2 = LikeFun::GetLikeFunction();
    Value value_pattern2(like_pattern2);
    std::shared_ptr<BoundConstantExpression> right_s_comment = std::make_shared<BoundConstantExpression>(value_pattern2);
    std::vector<std::shared_ptr<Expression>> arguments2;
    arguments2.push_back(std::move(p2_s_comment));
    arguments2.push_back(std::move(right_s_comment));
    std::shared_ptr<BoundFunctionExpression> functionExpression2 = std::make_shared<BoundFunctionExpression>(LogicalType::BOOL, bound_function2, arguments2, nullptr);
    functionExpression2->bind_info = functionExpression2->function.bind(functionExpression2->function, functionExpression2->children);
    // <s_suppkey, s_comment>
    std::shared_ptr<PhysicalFilter> filter2 = std::make_shared<PhysicalFilter>(functionExpression2);

    //  ================================================构建HashJoin================================================
    std::vector<int> build_ids1 = {0};
    std::vector<int> prode_ids1 = {0};
    // <p_partkey, p_brand, p_type, p_size, ps_partkey, ps_suppkey>
    std::shared_ptr<PhysicalHashJoin> join1 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids1, prode_ids1);
    join1->exp_name = "1";

    std::vector<int> build_ids2 = {0};
    std::vector<int> prode_ids2 = {5};
    // <p_partkey, p_brand, p_type, p_size, ps_partkey, ps_suppkey>
    std::shared_ptr<PhysicalHashJoin> join2 = std::make_shared<PhysicalHashJoin>(JoinTypes::ANTI, build_ids2, prode_ids2);
    join2->exp_name = "2";

    //  ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p_brand = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    std::shared_ptr<BoundColumnRefExpression> p_type = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 2, 0);
    std::shared_ptr<BoundColumnRefExpression> p_size = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 3, 0);
    std::shared_ptr<BoundColumnRefExpression> ps_suppkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 5, 0);
    std::vector<std::shared_ptr<Expression>> p_expressions = {p_brand, p_type, p_size, ps_suppkey};
    // < p_brand, p_type, p_size, ps_suppkey>
    std::shared_ptr<PhysicalProject> project1 = std::make_shared<PhysicalProject>(p_expressions);
    project1->exp_name = "1";

    // 构建HashAgg
    std::vector<int> group_set = {0, 1, 2};
    std::vector<int> agg_set = {3};
    std::vector<int> star_bitmap = {1};
    std::vector<AggFunctionType> aggregate_function_types = {AggFunctionType::COUNT};
    // <p_brand, p_type, p_size, Count(ps_suppkey)>
    std::shared_ptr<PhysicalMultiFieldHashAgg> hashAgg = std::make_shared<PhysicalMultiFieldHashAgg>(group_set, agg_set, star_bitmap, aggregate_function_types);

    //  ================================================构建OrderBy================================================
    std::vector<int> sort_keys = {3, 0, 1, 2};
    std::vector<LogicalType> key_types = {LogicalType::INTEGER, LogicalType::STRING, LogicalType::STRING, LogicalType::INTEGER};
    std::vector<SortOrder> orders_p = {SortOrder::DESCENDING, SortOrder::ASCENDING, SortOrder::ASCENDING, SortOrder::ASCENDING};
    std::shared_ptr<PhysicalOrder> orderby = std::make_shared<PhysicalOrder>(&sort_keys, &key_types, &orders_p);

    orderby->AddChild(hashAgg);
    hashAgg->AddChild(project1);
    project1->AddChild(join2);
    join2->AddChild(filter2);
    join2->AddChild(join1);
    filter2->AddChild(scan_supplier);
    join1->AddChild(filter1);
    join1->AddChild(scan_partsupp);
    filter1->AddChild(scan_part);

    std::shared_ptr<Pipeline> pipeline = std::make_shared<Pipeline>();
    std::shared_ptr<PipelineGroup> pipeline_group = std::make_shared<PipelineGroup>();
    orderby->build_pipelines(pipeline, pipeline_group);
    std::shared_ptr<PipelineGroupExecute> pipeline_group_executor = std::make_shared<PipelineGroupExecute>(scheduler, pipeline_group);
    pipeline_group_executor->traverse_plan();
    auto start_execute = std::chrono::steady_clock::now();
    pipeline_group_executor->execute();
    auto end_execute = std::chrono::steady_clock::now();
    spdlog::info("[{} : {}] Q16执行时间: {}", __FILE__, __LINE__, Util::time_difference(start_execute, end_execute));

    // 关闭线程池
    scheduler->shutdown();
    RC expected = RC::SUCCESS;
    ASSERT_EQ(expected, RC::SUCCESS);
}

// 测试4线程Q16查询
TEST(TPCHTest, Q16FourThreadTest) {
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

    std::cout << "Q16: select\n"
                 "    p_brand,\n"
                 "    p_type,\n"
                 "    p_size,\n"
                 "    count(ps_suppkey) as supplier_cnt\n"
                 "from\n"
                 "    partsupp,\n"
                 "    part\n"
                 "where\n"
                 "    p_partkey = ps_partkey\n"
                 "    and p_brand <> 'Brand#45'\n"
                 "    and p_type not like 'MEDIUM POLISHED%'\n"
                 "    and p_size in (49, 14, 23, 45, 19, 3, 36, 9)\n"
                 "    and ps_suppkey not in (\n"
                 "        select\n"
                 "            s_suppkey\n"
                 "        from\n"
                 "            supplier\n"
                 "        where\n"
                 "            s_comment like '%Customer%Complaints%'\n"
                 "    )\n"
                 "group by\n"
                 "    p_brand,\n"
                 "    p_type,\n"
                 "    p_size\n"
                 "order by\n"
                 "    supplier_cnt desc,\n"
                 "    p_brand,\n"
                 "    p_type,\n"
                 "    p_size;" << std::endl;

    //  ================================================构建Scan================================================
    std::vector<int> part_ids = {0, 3, 4, 5};
    // <p_partkey, p_brand, p_type, p_size>
    std::shared_ptr<PhysicalTableScan> scan_part = std::make_shared<PhysicalTableScan>(table_part, -1, part_ids);

    std::vector<int> partsupp_ids = {0, 1};
    // <ps_partkey, ps_suppkey>
    std::shared_ptr<PhysicalTableScan> scan_partsupp = std::make_shared<PhysicalTableScan>(table_partsupp, -1, partsupp_ids);

    std::vector<int> supplier_ids = {0, 6};
    // <s_suppkey, s_comment>
    std::shared_ptr<PhysicalTableScan> scan_supplier = std::make_shared<PhysicalTableScan>(table_supplier, -1, supplier_ids);

    //  ================================================构建Filter================================================
    std::shared_ptr<BoundColumnRefExpression> p1_p_brand = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    std::shared_ptr<BoundColumnRefExpression> p1_p_type = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 2, 0);
    std::shared_ptr<BoundColumnRefExpression> p1_p_size = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 3, 0);
    Value value_Brand_45("Brand#45");
    std::shared_ptr<BoundConstantExpression> right_p_brand = std::make_shared<BoundConstantExpression>(value_Brand_45);
    std::shared_ptr<BoundComparisonExpression> comparisonExpression = std::make_shared<BoundComparisonExpression>(ExpressionTypes::COMPARE_NOTEQUAL, p1_p_brand, right_p_brand);

    std::string like_pattern = "MEDIUM POLISHED%";
    ScalarFunction bound_function = NotLikeFun::GetNotLikeFunction();
    Value value_pattern(like_pattern);
    std::shared_ptr<BoundConstantExpression> right_p_type = std::make_shared<BoundConstantExpression>(value_pattern);
    std::vector<std::shared_ptr<Expression>> arguments;
    arguments.push_back(std::move(p1_p_type));
    arguments.push_back(std::move(right_p_type));
    std::shared_ptr<BoundFunctionExpression> functionExpression = std::make_shared<BoundFunctionExpression>(LogicalType::BOOL, bound_function, arguments, nullptr);
    functionExpression->bind_info = functionExpression->function.bind(functionExpression->function, functionExpression->children);

    std::vector<Value> value_set = {Value(49), Value(14), Value(23), Value(45), Value(19), Value(3), Value(36), Value(9)};
    std::shared_ptr<BoundInExpression> inExpression = std::make_shared<BoundInExpression>(p1_p_size, value_set, LogicalType::INTEGER);

    std::shared_ptr<BoundConjunctionExpression> conjunctionExpression1 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, comparisonExpression, functionExpression);
    std::shared_ptr<BoundConjunctionExpression> conjunctionExpression2 = std::make_shared<BoundConjunctionExpression>(ExpressionTypes::CONJUNCTION_AND, conjunctionExpression1, inExpression);
    // <p_partkey, p_brand, p_type, p_size>
    std::shared_ptr<PhysicalFilter> filter1 = std::make_shared<PhysicalFilter>(conjunctionExpression2);

    std::shared_ptr<BoundColumnRefExpression> p2_s_comment = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    std::string like_pattern2 = "%Customer%Complaints%";
    ScalarFunction bound_function2 = LikeFun::GetLikeFunction();
    Value value_pattern2(like_pattern2);
    std::shared_ptr<BoundConstantExpression> right_s_comment = std::make_shared<BoundConstantExpression>(value_pattern2);
    std::vector<std::shared_ptr<Expression>> arguments2;
    arguments2.push_back(std::move(p2_s_comment));
    arguments2.push_back(std::move(right_s_comment));
    std::shared_ptr<BoundFunctionExpression> functionExpression2 = std::make_shared<BoundFunctionExpression>(LogicalType::BOOL, bound_function2, arguments2, nullptr);
    functionExpression2->bind_info = functionExpression2->function.bind(functionExpression2->function, functionExpression2->children);
    // <s_suppkey, s_comment>
    std::shared_ptr<PhysicalFilter> filter2 = std::make_shared<PhysicalFilter>(functionExpression2);

    //  ================================================构建HashJoin================================================
    std::vector<int> build_ids1 = {0};
    std::vector<int> prode_ids1 = {0};
    // <p_partkey, p_brand, p_type, p_size, ps_partkey, ps_suppkey>
    std::shared_ptr<PhysicalHashJoin> join1 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids1, prode_ids1);
    join1->exp_name = "1";

    std::vector<int> build_ids2 = {0};
    std::vector<int> prode_ids2 = {5};
    // <p_partkey, p_brand, p_type, p_size, ps_partkey, ps_suppkey>
    std::shared_ptr<PhysicalHashJoin> join2 = std::make_shared<PhysicalHashJoin>(JoinTypes::ANTI, build_ids2, prode_ids2);
    join2->exp_name = "2";

    //  ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p_brand = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    std::shared_ptr<BoundColumnRefExpression> p_type = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 2, 0);
    std::shared_ptr<BoundColumnRefExpression> p_size = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 3, 0);
    std::shared_ptr<BoundColumnRefExpression> ps_suppkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 5, 0);
    std::vector<std::shared_ptr<Expression>> p_expressions = {p_brand, p_type, p_size, ps_suppkey};
    // < p_brand, p_type, p_size, ps_suppkey>
    std::shared_ptr<PhysicalProject> project1 = std::make_shared<PhysicalProject>(p_expressions);
    project1->exp_name = "1";

    // 构建HashAgg
    std::vector<int> group_set = {0, 1, 2};
    std::vector<int> agg_set = {3};
    std::vector<int> star_bitmap = {1};
    std::vector<AggFunctionType> aggregate_function_types = {AggFunctionType::COUNT};
    // <p_brand, p_type, p_size, Count(ps_suppkey)>
    std::shared_ptr<PhysicalMultiFieldHashAgg> hashAgg = std::make_shared<PhysicalMultiFieldHashAgg>(group_set, agg_set, star_bitmap, aggregate_function_types);

    //  ================================================构建OrderBy================================================
    std::vector<int> sort_keys = {3, 0, 1, 2};
    std::vector<LogicalType> key_types = {LogicalType::DOUBLE, LogicalType::STRING, LogicalType::STRING, LogicalType::INTEGER};
    std::vector<SortOrder> orders_p = {SortOrder::DESCENDING, SortOrder::ASCENDING, SortOrder::ASCENDING, SortOrder::ASCENDING};
    std::shared_ptr<PhysicalOrder> orderby = std::make_shared<PhysicalOrder>(&sort_keys, &key_types, &orders_p);

    orderby->AddChild(hashAgg);
    hashAgg->AddChild(project1);
    project1->AddChild(join2);
    join2->AddChild(filter2);
    join2->AddChild(join1);
    filter2->AddChild(scan_supplier);
    join1->AddChild(filter1);
    join1->AddChild(scan_partsupp);
    filter1->AddChild(scan_part);

    std::shared_ptr<Pipeline> pipeline = std::make_shared<Pipeline>();
    std::shared_ptr<PipelineGroup> pipeline_group = std::make_shared<PipelineGroup>();
    orderby->build_pipelines(pipeline, pipeline_group);
    std::shared_ptr<PipelineGroupExecute> pipeline_group_executor = std::make_shared<PipelineGroupExecute>(scheduler, pipeline_group);
    pipeline_group_executor->traverse_plan();
    pipeline_group_executor->execute();
    spdlog::info("[{} : {}] Q16执行时间: {} ms.", __FILE__, __LINE__, pipeline_group_executor->duration.count());
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