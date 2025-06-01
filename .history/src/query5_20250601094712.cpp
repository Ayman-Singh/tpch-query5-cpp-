#include "../include/query5.hpp"
#include <bits/stdc++.h>
#include <fstream>
#include <sstream>
#include <thread>
#include <mutex>
#include <algorithm>


using namespace std;

bool parseArgs(int argc, char* argv[], string& r_name, string& start_date, string& end_date, int& num_threads, string& table_path, string& result_path) {
  r_name="";
  start_date="";
  end_date="";
  num_threads=0;
  table_path="";
  result_path="";

for (int i=1;i<argc;i++){
    if (strcmp(argv[i],"--r_name")==0){
         if (i + 1 < argc) {
                r_name = argv[++i];
            } else {
                cerr << "Error: --r_name requires an argument\n";
                return false;
            }
     
    }
    else if (strcmp(argv[i],"--start_date")==0){
          if (i + 1 < argc) {
                start_date = argv[++i];
            } else {
                cerr << "Error: --start_date requires an argument\n";
                return false;
            }
    }
      else if (strcmp(argv[i],"--end_date")==0){
          if (i + 1 < argc) {
                end_date = argv[++i];
            } else {
                cerr << "Error: --send_date requires an argument\n";
                return false;
            }
    }
         else if (strcmp(argv[i],"--threads")==0) {
            if (i + 1 < argc) {
                try {
                    num_threads = stoi(argv[++i]);
                } catch (...) {
                    cerr << "Error: --threads requires an integer\n";
                    return false;
                }
            } else {
                cerr << "Error: --threads requires an argument\n";
                return false;
            }
        }

           else if (strcmp(argv[i],"--table_path")==0){
          if (i + 1 < argc) {
                table_path = argv[++i];
            } else {
                cerr << "Error: --table_path requires an argument\n";
                return false;
            }
    }

      else if (strcmp(argv[i],"--result_path")==0){
          if (i + 1 < argc) {
                result_path = argv[++i];
             } 
             else {
                cerr << "Error: --result_path requires an argument\n";
                return false;
            }
    }
    else {
            cerr << "Unknown argument: " << argv[i] << "\n";
            return false;
           }

}
    // TODO: Implement command line argument parsing
    // Example: --r_name ASIA --start_date 1994-01-01 --end_date 1995-01-01 --threads 4 --table_path /path/to/tables --result_path /path/to/results
    return true;
}

vector<vector<string>> readTable(const string& filepath) {
    vector<vector<string>> data;
    ifstream file(filepath);
    if (!file) return data;

    string line;
    while (getline(file, line)) {
        vector<string> row;
        stringstream ss(line);
        string field;
        while (getline(ss, field, '|')) {
            row.push_back(field);
        }
        data.push_back(row);
    }
    return data;
}

// Converts vector-of-vectors into a vector-of-maps
bool populateMapRows(const vector<vector<string>>& raw,
                     vector<map<string,string>>& out,
                     const vector<string>& cols)
{
    if (raw.empty()) return false;
    for (auto& vec : raw) {
        map<string,string> row;
        for (size_t i = 0; i < cols.size() && i < vec.size(); ++i) {
            if (!cols[i].empty()) {
                row[cols[i]] = vec[i];
            }
        }
        out.push_back(row);
    }
    return true;
}

bool readTPCHData(const string& table_path, vector<map<string,string>>& customer_data, vector<map<string,string>>& orders_data, vector<map<string,string>>& lineitem_data,vector<map<string,string>>& supplier_data,vector<map<string,string>>& nation_data,vector<map<string,string>>& region_data)
{
    //  columns [r_regionkey, r_name]
    auto rawRegion = readTable(table_path + "/region.tbl");
    if (!populateMapRows(rawRegion, region_data, {"r_regionkey","r_name"})) return false;

    // columns [n_nationkey, n_name, n_regionkey]
    auto rawNation = readTable(table_path + "/nation.tbl");
    if (!populateMapRows(rawNation, nation_data, {"n_nationkey","n_name","n_regionkey"})) return false;

    //cols [s_suppkey, −,−, s_nationkey] (only idx 0 and 3 matter)
    auto rawSupplier = readTable(table_path + "/supplier.tbl");
    if (!populateMapRows(rawSupplier, supplier_data, {"s_suppkey","","","s_nationkey"})) return false;

    // cols [c_custkey, −,−, c_nationkey]
    auto rawCustomer = readTable(table_path + "/customer.tbl");
    if (!populateMapRows(rawCustomer, customer_data, {"c_custkey","","","c_nationkey"})) return false;

    //cols [o_orderkey, o_custkey, −, −, o_orderdate]
    auto rawOrders = readTable(table_path + "/orders.tbl");
    if (!populateMapRows(rawOrders, orders_data, {"o_orderkey","o_custkey","","","o_orderdate"})) return false;

    //cols [l_orderkey, −, l_suppkey, −, −, l_extendedprice, l_discount]
    auto rawLineitem = readTable(table_path + "/lineitem.tbl");
    if (!populateMapRows(rawLineitem, lineitem_data,
                         {"l_orderkey","","l_suppkey","","","l_extendedprice","l_discount"}))
    {
        return false;
    }

    return true;
}


// Function to execute TPCH Query 5 using multithreading
bool executeQuery5(const string& r_name, const string& start_date, const string& end_date, int num_threads, const vector<map<string, string>>& customer_data, const vector<map<string, string>>& orders_data, const vector<map<string,string>>& lineitem_data, const vector<map<string, string>>& supplier_data, const vector<map<string,string>>& nation_data, const vector<map<string,string>>& region_data, map<string, double>& results) {
    int targetRegionKey = -1;
    for (auto &row : region_data) {
        int rkey  = stoi(row.at("r_regionkey"));
        const string &rnm = row.at("r_name");
        if (rnm == r_name) {
            targetRegionKey = rkey;
            break;
        }
    }
    if (targetRegionKey < 0) {
        cerr << "Error: Region '" << r_name << "' not found.\n";
        return false;
    }
   
    unordered_set<int> nationsInRegion;
    unordered_map<int, string> nationNameByNationKey;  //nationkey->nationame
    for (auto &row : nation_data) {
        int nkey      = stoi(row.at("n_nationkey"));
        int regionkey = stoi(row.at("n_regionkey"));
        if (regionkey == targetRegionKey) {
            nationsInRegion.insert(nkey);
            nationNameByNationKey[nkey] = row.at("n_name");
        }
    }
    if (nationsInRegion.empty()) {
        cerr << "Error: No nations found for region '" << r_name << "'.\n";
        return false;
    }

    
    unordered_map<int,int> supplierNation; // s_suppkey → s_nationkey
    for (auto &row : supplier_data) {
        int skey = stoi(row.at("s_suppkey"));
        int nkey = stoi(row.at("s_nationkey"));
        if (nationsInRegion.count(nkey) > 0) {
            supplierNation[skey] = nkey;
        }
    }
    if (supplierNation.empty()) {
        cerr << "Error: No suppliers found in region '" << r_name << "'.\n";
        return false;
    }

    // 4) Filter customers whose c_nationkey is in our region
    unordered_map<int,int> customerNation; // c_custkey → c_nationkey
    for (auto &row : customer_data) {
        int ckey = stoi(row.at("c_custkey"));
        int nkey = stoi(row.at("c_nationkey"));
        if (nationsInRegion.count(nkey) > 0) {
            customerNation[ckey] = nkey;
        }
    }
    if (customerNation.empty()) {
        cerr << "Error: No customers found in region '" << r_name << "'.\n";
        return false;
    }

    // 5) Filter orders: keep only those whose customer is in our region and date in [start,end)
    unordered_map<int,int> orderCustKey; // o_orderkey → o_custkey
    for (auto &row : orders_data) {
        int okey = stoi(row.at("o_orderkey"));
        int ckey = stoi(row.at("o_custkey"));
        const string &odate = row.at("o_orderdate");
        if (customerNation.count(ckey) > 0) {
            if (odate >= start_date && odate < end_date) {
                orderCustKey[okey] = ckey;
            }
        }
    }
    if (orderCustKey.empty()) {
        cerr << "Error: No orders found for customers in region '" << r_name
             << "' during the given date range.\n";
        return false;
    }

    // 6) Scan lineitem_data in parallel, accumulating rev/nation
    unordered_map<string,double> sharedRevenue;
    mutex mtx;

    auto worker = [&](int startIdx, int endIdx) {
        for (int i = startIdx; i < endIdx; ++i) {
            const auto &row = lineitem_data[i];
            int okey = stoi(row.at("l_orderkey"));
            int skey = stoi(row.at("l_suppkey"));

            // Only proceed if this lineitem’s order is in orderCustKey and its supplier is in our region
            if (orderCustKey.count(okey) == 0) continue;
            if (supplierNation.count(skey) == 0) continue;

            double extprice = stod(row.at("l_extendedprice"));
            double discount = stod(row.at("l_discount"));
            double rev = extprice * (1.0 - discount);

            int suppNationKey = supplierNation[skey];
            string nationName = nationNameByNationKey[suppNationKey];

            // safely done into sharedRevenue using locks
            {
                lock_guard<mutex> lock(mtx);
                sharedRevenue[nationName] += rev;
            }
        }
    };

    int N = static_cast<int>(lineitem_data.size());
    vector<thread> threads;
    threads.reserve(num_threads);

    int baseSize  = N / num_threads;
    int remainder = N % num_threads;
    int offset    = 0;

    for (int t = 0; t < num_threads; ++t) {
        int chunk = baseSize + (t < remainder ? 1 : 0);
        int startIdx = offset;
        int endIdx   = offset + chunk;
        if (endIdx > N) endIdx = N;
        threads.emplace_back(worker, startIdx, endIdx);
        offset = endIdx;
    }

    for (auto &th : threads) {
        th.join();
    }

    // 7) Copy sharedRevenue into the final results map (sorted by nation name)
    for (auto &kv : sharedRevenue) {
        results[kv.first] = kv.second;
    }

    return true;
 
    // TODO: Implement TPCH Query 5 using multithreading
    
}

// Function to output results to the specified path
bool outputResults(const string& result_path, const map<string, double>& results) {
    vector<pair<string,double>> vec;
    vec.reserve(results.size());
    for (const auto &kv : results) {
        vec.push_back(kv); // (nation, revenue)
    }

    sort(vec.begin(), vec.end(),
         [](const auto &a, const auto &b) {
             return a.second > b.second;
         });

    ofstream fout(result_path);
    if (!fout.is_open()) {
        cerr << "Cannot open result file: " << result_path << "\n";
        return false;
    }

    fout << fixed << setprecision(2);
    for (const auto &kv : vec) {
        fout << kv.first << "|" << kv.second << "\n";
    }
    fout.close();
    return true;
    // TODO: Implement outputting results to a file
    
} 