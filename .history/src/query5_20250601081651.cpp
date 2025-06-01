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
            } else {
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

bool readTPCHData(const string& table_path,
                  vector<map<string, string>>& customer_data,
                  vector<map<string, string>>& orders_data,
                  vector<map<string, string>>& lineitem_data,
                  vector<map<string, string>>& supplier_data,
                  vector<map<string, string>>& nation_data,
                  vector<map<string, string>>& region_data)
{
    // Build full file paths
    string region_file   = table_path + "/region.tbl";
    string nation_file   = table_path + "/nation.tbl";
    string supplier_file = table_path + "/supplier.tbl";
    string customer_file = table_path + "/customer.tbl";
    string orders_file   = table_path + "/orders.tbl";
    string lineitem_file = table_path + "/lineitem.tbl";
 
    // For region table

    {
        ifstream in(region_file);
        if (!in.is_open()) {
            cerr << "Cannot open " << region_file << "\n";
            return false;
        }
        string line;
        while (getline(in, line)) {
            stringstream ss(line);
            string token;
            vector<string> tokens;
            while (getline(ss, token, '|')) {
                tokens.push_back(token);
            }
            // region has at least 2 tokens: [0]=r_regionkey, [1]=r_name
            if (tokens.size() >= 2) {
                map<string, string> row;
                row["r_regionkey"] = tokens[0];
                row["r_name"]      = tokens[1];
                region_data.push_back(row);
            }
        }
        in.close();
    }

    // For nation table
    {
        ifstream in(nation_file);
        if (!in.is_open()) {
            cerr << "Cannot open " << nation_file << "\n";
            return false;
        }
        string line;
        while (getline(in, line)) {
            stringstream ss(line);
            string token;
            vector<string> tokens;
            while (getline(ss, token, '|')) {
                tokens.push_back(token);
            }
            // nation has at least 3 tokens: [0]=n_nationkey, [1]=n_name, [2]=n_regionkey
            if (tokens.size() >= 3) {
                map<string, string> row;
                row["n_nationkey"]  = tokens[0];
                row["n_name"]       = tokens[1];
                row["n_regionkey"]  = tokens[2];
                nation_data.push_back(row);
            }
        }
        in.close();
    }

    // for supplier table
    {
        ifstream in(supplier_file);
        if (!in.is_open()) {
            cerr << "Cannot open " << supplier_file << "\n";
            return false;
        }
        string line;
        while (getline(in, line)) {
            stringstream ss(line);
            string token;
            vector<string> tokens;
            while (getline(ss, token, '|')) {
                tokens.push_back(token);
            }
            // supplier has at least 4 tokens: [0]=s_suppkey, [3]=s_nationkey
            if (tokens.size() >= 4) {
                map<string, string> row;
                row["s_suppkey"]   = tokens[0];
                row["s_nationkey"] = tokens[3];
                supplier_data.push_back(row);
            }
        }
        in.close();
    }

    // for customer tble
    {
        ifstream in(customer_file);
        if (!in.is_open()) {
            cerr << "Cannot open " << customer_file << "\n";
            return false;
        }
        string line;
        while (getline(in, line)) {
            stringstream ss(line);
            string token;
            vector<string> tokens;
            while (getline(ss, token, '|')) {
                tokens.push_back(token);
            }
            // customer has at least 4 tokens: [0]=c_custkey, [3]=c_nationkey
            if (tokens.size() >= 4) {
                map<string, string> row;
                row["c_custkey"]   = tokens[0];
                row["c_nationkey"] = tokens[3];
                customer_data.push_back(row);
            }
        }
        in.close();
    }

    // For orders table
    {
        ifstream in(orders_file);
        if (!in.is_open()) {
            cerr << "Cannot open " << orders_file << "\n";
            return false;
        }
        string line;
        while (getline(in, line)) {
            stringstream ss(line);
            string token;
            vector<string> tokens;
            while (getline(ss, token, '|')) {
                tokens.push_back(token);
            }
            // orders has at least 5 tokens: [0]=o_orderkey, [1]=o_custkey, [4]=o_orderdate
            if (tokens.size() >= 5) {
                map<string, string> row;
                row["o_orderkey"]  = tokens[0];
                row["o_custkey"]   = tokens[1];
                row["o_orderdate"] = tokens[4];
                orders_data.push_back(row);
            }
        }
        in.close();
    }

    // lineitem table
    {
        ifstream in(lineitem_file);
        if (!in.is_open()) {
            cerr << "Cannot open " << lineitem_file << "\n";
            return false;
        }
        string line;
        while (getline(in, line)) {
            stringstream ss(line);
            string token;
            vector<string> tokens;
            while (getline(ss, token, '|')) {
                tokens.push_back(token);
            }
            // lineitem has at least 7 tokens:
            // [0]=l_orderkey, [2]=l_suppkey, [5]=l_extendeprice, [6]=l_discount
            if (tokens.size() >= 7) {
                map<string, string> row;
                row["l_orderkey"]      = tokens[0];
                row["l_suppkey"]       = tokens[2];
                row["l_extendedprice"] = tokens[5];
                row["l_discount"]      = tokens[6];
                lineitem_data.push_back(row);
            }
        }
        in.close();
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

    // 6) Scan lineitem_data in parallel, accumulating revenue per nation
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

            // Safely accumulate into sharedRevenue
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