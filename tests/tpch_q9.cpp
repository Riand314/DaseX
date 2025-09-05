#include "import_data.hpp"
#include "file_dir_config.hpp"
// =============================Q9=============================
// 测试单线程Q9查询
TEST(TPCHTest, Q9SingleThreadTest) {
    std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
    bool importFinish = false;
    std::string fileDir = fileDir1;
    // 插入Supplier表数据
    std::shared_ptr<Table> table_supplier;
    InsertSupplier(table_supplier, scheduler, fileDir, importFinish);
    importFinish = false;
    spdlog::info("[{} : {}] InsertSupplier Finish!!!", __FILE__, __LINE__);

    // 插入Nation表数据
    std::shared_ptr<Table> table_nation;
    InsertNation(table_nation, scheduler, fileDir, importFinish);
    importFinish = false;
    spdlog::info("[{} : {}] InsertNation Finish!!!", __FILE__, __LINE__);

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

    // 插入Lineitem表数据
    std::shared_ptr<Table> table_lineitem;
    InsertLineitem(table_lineitem, scheduler, fileDir, importFinish);
    importFinish = false;
    spdlog::info("[{} : {}] InsertLineitem Finish!!!", __FILE__, __LINE__);

    // 插入Orders表数据
    std::shared_ptr<Table> table_orders;
    InsertOrders(table_orders, scheduler, fileDir, importFinish);
    importFinish = false;
    spdlog::info("[{} : {}] InsertOrders Finish!!!", __FILE__, __LINE__);

    //  ================================================构建Scan================================================
    std::vector<int> nation_ids = {0, 1};
    // <n_nationkey, n_name>
    std::shared_ptr<PhysicalTableScan> scan_nation = std::make_shared<PhysicalTableScan>(table_nation, -1, nation_ids);

    std::vector<int> supplier_ids = {0, 3};
    // <s_suppkey, s_nationkey>
    std::shared_ptr<PhysicalTableScan> scan_supplier = std::make_shared<PhysicalTableScan>(table_supplier, -1, supplier_ids);

    std::vector<int> part_ids = {0, 1};
    // <p_partkey, p_name>
    std::shared_ptr<PhysicalTableScan> scan_part = std::make_shared<PhysicalTableScan>(table_part, -1, part_ids);

    std::vector<int> partsupp_ids = {0, 1, 3};
    // <ps_partkey, ps_suppkey, ps_supplycost>
    std::shared_ptr<PhysicalTableScan> scan_partsupp = std::make_shared<PhysicalTableScan>(table_partsupp, -1, partsupp_ids);

    std::vector<int> lineitem_ids = {0, 1, 2, 4, 5, 6};
    // <l_orderkey, l_partkey, l_suppkey, l_quantity, l_extendedprice, l_discount>
    std::shared_ptr<PhysicalTableScan> scan_lineitem = std::make_shared<PhysicalTableScan>(table_lineitem, -1, lineitem_ids);

    std::vector<int> orders_ids = {0, 4};
    // <o_orderkey, o_orderdate>
    std::shared_ptr<PhysicalTableScan> scan_orders = std::make_shared<PhysicalTableScan>(table_orders, -1, orders_ids);

    //  ================================================构建HashJoin================================================
    std::vector<int> build_ids5 = {0};
    std::vector<int> prode_ids5 = {1};
    // <n_nationkey, n_name, s_suppkey, s_nationkey>
    std::shared_ptr<PhysicalHashJoin> join_5 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids5, prode_ids5);
    join_5->exp_name = "5";

    std::vector<int> build_ids4 = {0};
    std::vector<int> prode_ids4 = {0};
    // <p_partkey, ps_partkey, ps_suppkey, ps_supplycost>
    std::shared_ptr<PhysicalHashJoin> join_4 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids4, prode_ids4);
    join_4->exp_name = "4";

    std::shared_ptr<BoundColumnRefExpression> p4_p_partkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 0, 0);
    std::vector<std::shared_ptr<Expression>> p4_expressions = {p4_p_partkey};
    // <p_partkey>
    std::shared_ptr<PhysicalProject> project4 = std::make_shared<PhysicalProject>(p4_expressions);

    std::string like_pattern = "%green%";
    ScalarFunction bound_function = LikeFun::GetLikeFunction();
    std::shared_ptr<BoundColumnRefExpression> left_p_name = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    Value value(like_pattern);
    std::shared_ptr<BoundConstantExpression> right_p_name = std::make_shared<BoundConstantExpression>(value);
    std::vector<std::shared_ptr<Expression>> arguments;
    arguments.push_back(std::move(left_p_name));
    arguments.push_back(std::move(right_p_name));
    std::shared_ptr<BoundFunctionExpression> functionExpression = std::make_shared<BoundFunctionExpression>(LogicalType::BOOL, bound_function, arguments, nullptr);
    functionExpression->bind_info = functionExpression->function.bind(functionExpression->function, functionExpression->children);
    // <p_partkey, p_name>
    std::shared_ptr<PhysicalFilter> filter_part = std::make_shared<PhysicalFilter>(functionExpression);
    filter_part->exp_name = "filter-part";

    std::vector<int> build_ids3 = {2};
    std::vector<int> prode_ids3 = {2};
    // <n_nationkey, n_name, s_suppkey, s_nationkey, p_partkey, ps_partkey, ps_suppkey, ps_supplycost>
    std::shared_ptr<PhysicalHashJoin> join_3 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids3, prode_ids3);
    join_3->exp_name = "3";

    std::vector<int> build_ids2 = {6, 5, 4, 2};
    std::vector<int> prode_ids2 = {2, 1, 1, 2};
    // <n_nationkey, n_name, s_suppkey, s_nationkey, p_partkey, ps_partkey, ps_suppkey, ps_supplycost, l_orderkey, l_partkey, l_suppkey, l_quantity, l_extendedprice, l_discount>
    std::shared_ptr<PhysicalHashJoin> join_2 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids2, prode_ids2);
    join_2->exp_name = "2";

    std::vector<int> build_ids1 = {8};
    std::vector<int> prode_ids1 = {0};
    // <n_nationkey, n_name, s_suppkey, s_nationkey, p_partkey, ps_partkey, ps_suppkey, ps_supplycost, l_orderkey, l_partkey, l_suppkey, l_quantity, l_extendedprice, l_discount, o_orderkey, o_orderdate>
    std::shared_ptr<PhysicalHashJoin> join_1 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids1, prode_ids1);
    join_1->exp_name = "1";

    // ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p3_n_name = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    std::shared_ptr<BoundColumnRefExpression> p3_o_orderdate = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 15, 0);
    std::shared_ptr<BoundColumnRefExpression> p3_l_quantity = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 11, 0);
    std::shared_ptr<BoundColumnRefExpression> p3_l_extendedprice = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 12, 0);
    std::shared_ptr<BoundColumnRefExpression> p3_l_discount = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 13, 0);
    std::shared_ptr<BoundColumnRefExpression> p3_ps_supplycost = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 7, 0);

    std::vector<LogicalType> p3_arguments1 = { LogicalType::FLOAT, LogicalType::FLOAT };
    scalar_function_p p3_function1 = GetSubFunction();
    ScalarProjectFunction p3_bound_function1("-", p3_arguments1, LogicalType::FLOAT, p3_function1, nullptr);
    Value value_1(1.0f);
    std::shared_ptr<BoundConstantExpression> constant1_col = std::make_shared<BoundConstantExpression>(value_1);
    std::vector<std::shared_ptr<Expression>> p3_expressions1 = {constant1_col, p3_l_discount};
    std::shared_ptr<BoundProjectFunctionExpression> discount_1_col = std::make_shared<BoundProjectFunctionExpression>(LogicalType::FLOAT, p3_bound_function1, p3_expressions1, nullptr);
    discount_1_col->alias = "1-l_discount";

    std::vector<LogicalType> p3_arguments2 = { LogicalType::FLOAT, LogicalType::FLOAT };
    scalar_function_p p3_function2 = GetMulFunction();
    ScalarProjectFunction p3_bound_function2("*", p3_arguments2, LogicalType::FLOAT, p3_function2, nullptr);
    std::vector<std::shared_ptr<Expression>> p3_expressions2 = {p3_l_extendedprice, discount_1_col};
    std::shared_ptr<BoundProjectFunctionExpression> extendedprice_discount_1_col = std::make_shared<BoundProjectFunctionExpression>(LogicalType::FLOAT, p3_bound_function2, p3_expressions2, nullptr);
    extendedprice_discount_1_col->alias = "l_extendedprice * (1 - l_discount)";

    std::vector<LogicalType> p3_arguments3 = { LogicalType::FLOAT, LogicalType::FLOAT };
    scalar_function_p p3_function3 = GetMulFunction();
    ScalarProjectFunction p3_bound_function3("*", p3_arguments3, LogicalType::FLOAT, p3_function3, nullptr);
    std::vector<std::shared_ptr<Expression>> p3_expressions3 = {p3_ps_supplycost, p3_l_quantity};
    std::shared_ptr<BoundProjectFunctionExpression> ps_supplycost_l_quantity = std::make_shared<BoundProjectFunctionExpression>(LogicalType::FLOAT, p3_bound_function3, p3_expressions3, nullptr);
    ps_supplycost_l_quantity->alias = "ps_supplycost * l_quantity";

    std::vector<LogicalType> p3_arguments4 = { LogicalType::FLOAT, LogicalType::FLOAT };
    scalar_function_p p3_function4 = GetSubFunction();
    ScalarProjectFunction p3_bound_function4("-", p3_arguments4, LogicalType::FLOAT, p3_function4, nullptr);
    std::vector<std::shared_ptr<Expression>> p3_expressions4 = {extendedprice_discount_1_col, ps_supplycost_l_quantity};
    std::shared_ptr<BoundProjectFunctionExpression> amount = std::make_shared<BoundProjectFunctionExpression>(LogicalType::FLOAT, p3_bound_function4, p3_expressions4, nullptr);
    amount->alias = "amount";

    std::vector<LogicalType> p3_arguments5 = { LogicalType::INTEGER, LogicalType::INTEGER };
    scalar_function_p p3_function5 = GetExtractYearFunction();
    ScalarProjectFunction p3_bound_function5("extract_year", p3_arguments5, LogicalType::INTEGER, p3_function5, nullptr);
    std::vector<std::shared_ptr<Expression>> p3_expressions5 = {p3_o_orderdate, p3_o_orderdate};
    std::shared_ptr<BoundProjectFunctionExpression> o_year = std::make_shared<BoundProjectFunctionExpression>(LogicalType::INTEGER, p3_bound_function5, p3_expressions5, nullptr);
    discount_1_col->alias = "o_year";

    std::vector<std::shared_ptr<Expression>> p3_expressions6 = {p3_n_name, o_year, amount};
    // <n_name(nation), o_year, amount>
    std::shared_ptr<PhysicalProject> project3 = std::make_shared<PhysicalProject>(p3_expressions6);

    // 构建HashAgg
    std::vector<int> group_set = {0, 1};
    std::vector<int> agg_set = {2};
    std::vector<int> star_bitmap = {0};
    std::vector<AggFunctionType> aggregate_function_types = {AggFunctionType::SUM};
    // <nation, o_year, Sum(amount)>
    std::shared_ptr<PhysicalMultiFieldHashAgg> hashAgg = std::make_shared<PhysicalMultiFieldHashAgg>(group_set, agg_set, star_bitmap, aggregate_function_types);

    //  ================================================构建OrderBy================================================
    std::vector<int> sort_keys = {0, 1};
    std::vector<LogicalType> key_types = {LogicalType::STRING, LogicalType::INTEGER};
    std::vector<SortOrder> orders_p = {SortOrder::ASCENDING, SortOrder::DESCENDING};
    std::shared_ptr<PhysicalOrder> orderby = std::make_shared<PhysicalOrder>(&sort_keys, &key_types, &orders_p);

    orderby->AddChild(hashAgg);
    hashAgg->AddChild(project3);
    project3->AddChild(join_1);
    join_1->AddChild(join_2);
    join_1->AddChild(scan_orders);
    join_2->AddChild(join_3);
    join_2->AddChild(scan_lineitem);
    join_3->AddChild(join_5);
    join_3->AddChild(join_4);
    join_5->AddChild(scan_nation);
    join_5->AddChild(scan_supplier);
    join_4->AddChild(project4);
    join_4->AddChild(scan_partsupp);
    project4->AddChild(filter_part);
    filter_part->AddChild(scan_part);

    std::shared_ptr<Pipeline> pipeline = std::make_shared<Pipeline>();
    std::shared_ptr<PipelineGroup> pipeline_group = std::make_shared<PipelineGroup>();
    orderby->build_pipelines(pipeline, pipeline_group);
    std::shared_ptr<PipelineGroupExecute> pipeline_group_executor = std::make_shared<PipelineGroupExecute>(scheduler, pipeline_group);
    pipeline_group_executor->traverse_plan();
    auto start_execute = std::chrono::steady_clock::now();
    pipeline_group_executor->execute();
    auto end_execute = std::chrono::steady_clock::now();
    spdlog::info("[{} : {}] Q9执行时间: {}", __FILE__, __LINE__, Util::time_difference(start_execute, end_execute));

    // 关闭线程池
    scheduler->shutdown();
    RC expected = RC::SUCCESS;
    ASSERT_EQ(expected, RC::SUCCESS);
}

// 测试4线程Q9查询
TEST(TPCHTest, Q9FourThreadTest) {
    std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(72);
    bool importFinish = false;
    std::string fileDir = fileDir4;
    std::vector<int> work_ids(arr, arr + sizeof(arr) / sizeof(arr[0]));
    std::vector<int> partition_idxs(arr, arr + sizeof(arr) / sizeof(arr[0]));
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

    // 插入Nation表数据
    std::shared_ptr<Table> table_nation;
    std::vector<std::string> nation_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "nation.tbl_" + std::to_string(i);
        nation_file_names.emplace_back(file_name);
    }
    InsertNationMul(table_nation, scheduler, work_ids, partition_idxs, nation_file_names, importFinish);
    importFinish = false;
    std::this_thread::sleep_for(std::chrono::seconds(1));
    // spdlog::info("[{} : {}] InsertNation Finish!!!", __FILE__, __LINE__);

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

    // 插入Lineitem表数据
    std::shared_ptr<Table> table_lineitem;
    std::vector<std::string> lineitem_file_names;
    for(int i = 0; i < work_ids.size(); i++) {
        std::string file_name = fileDir + "/" + "lineitem.tbl_" + std::to_string(i);
        lineitem_file_names.emplace_back(file_name);
    }
    InsertLineitemMul(table_lineitem, scheduler, work_ids, partition_idxs, lineitem_file_names, importFinish);
    importFinish = false;
    std::this_thread::sleep_for(std::chrono::seconds(3));
    // spdlog::info("[{} : {}] InsertLineitem Finish!!!", __FILE__, __LINE__);

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

    std::cout << "Q9: select\n"
                 "    nation,\n"
                 "    o_year,\n"
                 "    sum(amount) as sum_profit\n"
                 "from(\n"
                 "  select\n"
                 "        n_name as nation,\n"
                 "        extract(year from o_orderdate) as o_year,\n"
                 "        l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount\n"
                 "    from\n"
                 "        part,supplier,lineitem,partsupp,orders,nation\n"
                 "    where\n"
                 "        s_suppkey = l_suppkey\n"
                 "        and ps_suppkey = l_suppkey\n"
                 "        and ps_partkey = l_partkey\n"
                 "        and p_partkey = l_partkey\n"
                 "        and o_orderkey = l_orderkey\n"
                 "        and s_nationkey = n_nationkey\n"
                 "        and p_name like '%green%'\n"
                 ") as profit\n"
                 "group by\n"
                 "    nation,\n"
                 "    o_year\n"
                 "order by\n"
                 "    nation,\n"
                 "    o_year desc;" << std::endl;

    //  ================================================构建Scan================================================
    std::vector<int> nation_ids = {0, 1};
    // <n_nationkey, n_name>
    std::shared_ptr<PhysicalTableScan> scan_nation = std::make_shared<PhysicalTableScan>(table_nation, -1, nation_ids);

    std::vector<int> supplier_ids = {0, 3};
    // <s_suppkey, s_nationkey>
    std::shared_ptr<PhysicalTableScan> scan_supplier = std::make_shared<PhysicalTableScan>(table_supplier, -1, supplier_ids);

    std::vector<int> part_ids = {0, 1};
    // <p_partkey, p_name>
    std::shared_ptr<PhysicalTableScan> scan_part = std::make_shared<PhysicalTableScan>(table_part, -1, part_ids);

    std::vector<int> partsupp_ids = {0, 1, 3};
    // <ps_partkey, ps_suppkey, ps_supplycost>
    std::shared_ptr<PhysicalTableScan> scan_partsupp = std::make_shared<PhysicalTableScan>(table_partsupp, -1, partsupp_ids);

    std::vector<int> lineitem_ids = {0, 1, 2, 4, 5, 6};
    // <l_orderkey, l_partkey, l_suppkey, l_quantity, l_extendedprice, l_discount>
    std::shared_ptr<PhysicalTableScan> scan_lineitem = std::make_shared<PhysicalTableScan>(table_lineitem, -1, lineitem_ids);

    std::vector<int> orders_ids = {0, 4};
    // <o_orderkey, o_orderdate>
    std::shared_ptr<PhysicalTableScan> scan_orders = std::make_shared<PhysicalTableScan>(table_orders, -1, orders_ids);

    //  ================================================构建HashJoin================================================
    std::vector<int> build_ids5 = {0};
    std::vector<int> prode_ids5 = {1};
    std::vector<ExpressionTypes> comparison_types5 = {};
    // <n_nationkey, n_name, s_suppkey, s_nationkey>
    std::shared_ptr<PhysicalHashJoin> join_5 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids5, prode_ids5, comparison_types5, true);
    join_5->exp_name = "5";

    std::vector<int> build_ids4 = {0};
    std::vector<int> prode_ids4 = {0};
    // <p_partkey, ps_partkey, ps_suppkey, ps_supplycost>
    std::shared_ptr<PhysicalHashJoin> join_4 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids4, prode_ids4);
    join_4->exp_name = "4";

    std::shared_ptr<BoundColumnRefExpression> p4_p_partkey = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 0, 0);
    std::vector<std::shared_ptr<Expression>> p4_expressions = {p4_p_partkey};
    // <p_partkey>
    std::shared_ptr<PhysicalProject> project4 = std::make_shared<PhysicalProject>(p4_expressions);

    std::string like_pattern = "%green%";
    ScalarFunction bound_function = LikeFun::GetLikeFunction();
    std::shared_ptr<BoundColumnRefExpression> left_p_name = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    Value value(like_pattern);
    std::shared_ptr<BoundConstantExpression> right_p_name = std::make_shared<BoundConstantExpression>(value);
    std::vector<std::shared_ptr<Expression>> arguments;
    arguments.push_back(std::move(left_p_name));
    arguments.push_back(std::move(right_p_name));
    std::shared_ptr<BoundFunctionExpression> functionExpression = std::make_shared<BoundFunctionExpression>(LogicalType::BOOL, bound_function, arguments, nullptr);
    functionExpression->bind_info = functionExpression->function.bind(functionExpression->function, functionExpression->children);
    // <p_partkey, p_name>
    std::shared_ptr<PhysicalFilter> filter_part = std::make_shared<PhysicalFilter>(functionExpression);
    filter_part->exp_name = "filter-part";

    std::vector<int> build_ids3 = {2};
    std::vector<int> prode_ids3 = {2};
    // <n_nationkey, n_name, s_suppkey, s_nationkey, p_partkey, ps_partkey, ps_suppkey, ps_supplycost>
    std::shared_ptr<PhysicalHashJoin> join_3 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids3, prode_ids3);
    join_3->exp_name = "3";

    std::vector<int> build_ids2 = {6, 5, 4, 2};
    std::vector<int> prode_ids2 = {2, 1, 1, 2};
    // <n_nationkey, n_name, s_suppkey, s_nationkey, p_partkey, ps_partkey, ps_suppkey, ps_supplycost, l_orderkey, l_partkey, l_suppkey, l_quantity, l_extendedprice, l_discount>
    std::shared_ptr<PhysicalHashJoin> join_2 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids2, prode_ids2);
    join_2->exp_name = "2";

    std::vector<int> build_ids1 = {8};
    std::vector<int> prode_ids1 = {0};
    // <n_nationkey, n_name, s_suppkey, s_nationkey, p_partkey, ps_partkey, ps_suppkey, ps_supplycost, l_orderkey, l_partkey, l_suppkey, l_quantity, l_extendedprice, l_discount, o_orderkey, o_orderdate>
    std::shared_ptr<PhysicalHashJoin> join_1 = std::make_shared<PhysicalHashJoin>(JoinTypes::INNER, build_ids1, prode_ids1);
    join_1->exp_name = "1";

    // ================================================构建Project================================================
    std::shared_ptr<BoundColumnRefExpression> p3_n_name = std::make_shared<BoundColumnRefExpression>(LogicalType::STRING, 0, 1, 0);
    std::shared_ptr<BoundColumnRefExpression> p3_o_orderdate = std::make_shared<BoundColumnRefExpression>(LogicalType::INTEGER, 0, 15, 0);
    std::shared_ptr<BoundColumnRefExpression> p3_l_quantity = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 11, 0);
    std::shared_ptr<BoundColumnRefExpression> p3_l_extendedprice = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 12, 0);
    std::shared_ptr<BoundColumnRefExpression> p3_l_discount = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 13, 0);
    std::shared_ptr<BoundColumnRefExpression> p3_ps_supplycost = std::make_shared<BoundColumnRefExpression>(LogicalType::FLOAT, 0, 7, 0);

    std::vector<LogicalType> p3_arguments1 = { LogicalType::FLOAT, LogicalType::FLOAT };
    scalar_function_p p3_function1 = GetSubFunction();
    ScalarProjectFunction p3_bound_function1("-", p3_arguments1, LogicalType::FLOAT, p3_function1, nullptr);
    Value value_1(1.0f);
    std::shared_ptr<BoundConstantExpression> constant1_col = std::make_shared<BoundConstantExpression>(value_1);
    std::vector<std::shared_ptr<Expression>> p3_expressions1 = {constant1_col, p3_l_discount};
    std::shared_ptr<BoundProjectFunctionExpression> discount_1_col = std::make_shared<BoundProjectFunctionExpression>(LogicalType::FLOAT, p3_bound_function1, p3_expressions1, nullptr);
    discount_1_col->alias = "1-l_discount";

    std::vector<LogicalType> p3_arguments2 = { LogicalType::FLOAT, LogicalType::FLOAT };
    scalar_function_p p3_function2 = GetMulFunction();
    ScalarProjectFunction p3_bound_function2("*", p3_arguments2, LogicalType::FLOAT, p3_function2, nullptr);
    std::vector<std::shared_ptr<Expression>> p3_expressions2 = {p3_l_extendedprice, discount_1_col};
    std::shared_ptr<BoundProjectFunctionExpression> extendedprice_discount_1_col = std::make_shared<BoundProjectFunctionExpression>(LogicalType::FLOAT, p3_bound_function2, p3_expressions2, nullptr);
    extendedprice_discount_1_col->alias = "l_extendedprice * (1 - l_discount)";

    std::vector<LogicalType> p3_arguments3 = { LogicalType::FLOAT, LogicalType::FLOAT };
    scalar_function_p p3_function3 = GetMulFunction();
    ScalarProjectFunction p3_bound_function3("*", p3_arguments3, LogicalType::FLOAT, p3_function3, nullptr);
    std::vector<std::shared_ptr<Expression>> p3_expressions3 = {p3_ps_supplycost, p3_l_quantity};
    std::shared_ptr<BoundProjectFunctionExpression> ps_supplycost_l_quantity = std::make_shared<BoundProjectFunctionExpression>(LogicalType::FLOAT, p3_bound_function3, p3_expressions3, nullptr);
    ps_supplycost_l_quantity->alias = "ps_supplycost * l_quantity";

    std::vector<LogicalType> p3_arguments4 = { LogicalType::FLOAT, LogicalType::FLOAT };
    scalar_function_p p3_function4 = GetSubFunction();
    ScalarProjectFunction p3_bound_function4("-", p3_arguments4, LogicalType::FLOAT, p3_function4, nullptr);
    std::vector<std::shared_ptr<Expression>> p3_expressions4 = {extendedprice_discount_1_col, ps_supplycost_l_quantity};
    std::shared_ptr<BoundProjectFunctionExpression> amount = std::make_shared<BoundProjectFunctionExpression>(LogicalType::FLOAT, p3_bound_function4, p3_expressions4, nullptr);
    amount->alias = "amount";

    std::vector<LogicalType> p3_arguments5 = { LogicalType::INTEGER, LogicalType::INTEGER };
    scalar_function_p p3_function5 = GetExtractYearFunction();
    ScalarProjectFunction p3_bound_function5("extract_year", p3_arguments5, LogicalType::INTEGER, p3_function5, nullptr);
    std::vector<std::shared_ptr<Expression>> p3_expressions5 = {p3_o_orderdate, p3_o_orderdate};
    std::shared_ptr<BoundProjectFunctionExpression> o_year = std::make_shared<BoundProjectFunctionExpression>(LogicalType::INTEGER, p3_bound_function5, p3_expressions5, nullptr);
    discount_1_col->alias = "o_year";

    std::vector<std::shared_ptr<Expression>> p3_expressions6 = {p3_n_name, o_year, amount};
    // <n_name(nation), o_year, amount>
    std::shared_ptr<PhysicalProject> project3 = std::make_shared<PhysicalProject>(p3_expressions6);

    // 构建HashAgg
    std::vector<int> group_set = {0, 1};
    std::vector<int> agg_set = {2};
    std::vector<int> star_bitmap = {0};
    std::vector<AggFunctionType> aggregate_function_types = {AggFunctionType::SUM};
    // <nation, o_year, Sum(amount)>
    std::shared_ptr<PhysicalMultiFieldHashAgg> hashAgg = std::make_shared<PhysicalMultiFieldHashAgg>(group_set, agg_set, star_bitmap, aggregate_function_types);

    //  ================================================构建OrderBy================================================
    std::vector<int> sort_keys = {0, 1};
    std::vector<LogicalType> key_types = {LogicalType::STRING, LogicalType::INTEGER};
    std::vector<SortOrder> orders_p = {SortOrder::ASCENDING, SortOrder::DESCENDING};
    std::shared_ptr<PhysicalOrder> orderby = std::make_shared<PhysicalOrder>(&sort_keys, &key_types, &orders_p);

    orderby->AddChild(hashAgg);
    hashAgg->AddChild(project3);
    project3->AddChild(join_1);
    join_1->AddChild(join_2);
    join_1->AddChild(scan_orders);
    join_2->AddChild(join_3);
    join_2->AddChild(scan_lineitem);
    join_3->AddChild(join_5);
    join_3->AddChild(join_4);
    join_5->AddChild(scan_nation);
    join_5->AddChild(scan_supplier);
    join_4->AddChild(project4);
    join_4->AddChild(scan_partsupp);
    project4->AddChild(filter_part);
    filter_part->AddChild(scan_part);

    std::shared_ptr<Pipeline> pipeline = std::make_shared<Pipeline>();
    std::shared_ptr<PipelineGroup> pipeline_group = std::make_shared<PipelineGroup>();
    orderby->build_pipelines(pipeline, pipeline_group);
    std::shared_ptr<PipelineGroupExecute> pipeline_group_executor = std::make_shared<PipelineGroupExecute>(scheduler, pipeline_group);
    pipeline_group_executor->traverse_plan();
    pipeline_group_executor->execute();
    spdlog::info("[{} : {}] Q9执行时间: {} ms.", __FILE__, __LINE__, pipeline_group_executor->duration.count());
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