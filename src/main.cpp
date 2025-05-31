// main.cpp
#include "../include/query5.hpp"
#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <mutex>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <map>
#include <chrono>
using namespace std;

int main(int argc, char* argv[]) {
    string r_name, start_date, end_date, table_path, result_path;
    int num_threads;
    if (!parseArgs(argc, argv, r_name, start_date, end_date, num_threads, table_path, result_path)) {
        cerr << "Failed to parse command line arguments." << endl;
        return 1;
    }
    vector<map<string, string>> customer_data, orders_data, lineitem_data, supplier_data, nation_data, region_data;
    if (!readTPCHData(table_path, customer_data, orders_data, lineitem_data, supplier_data, nation_data, region_data)) {
        cerr << "Failed to read TPCH data." << endl;
        return 1;
    }
    map<string, double> results;
    auto start_time = chrono::high_resolution_clock::now();
    if (!executeQuery5(r_name, start_date, end_date, num_threads, customer_data, orders_data, lineitem_data, supplier_data, nation_data, region_data, results)) {
        cerr << "Failed to execute TPCH Query 5." << endl;
        return 1;
    }
    auto end_time = chrono::high_resolution_clock::now();
    chrono::duration<double> elapsed = end_time - start_time;
    cout << "Execution time with " << num_threads << " thread(s): " << elapsed.count() << " seconds." << endl;
    if (!outputResults(result_path, results)) {
        cerr << "Failed to output results." << endl;
        return 1;
    }
    cout << "TPCH Query 5 implementation completed." << endl;
    return 0;
}
