#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <algorithm>
#include <queue>
#include <sstream>
#include <ctime>
#include <cstdio>
#include <cstring>

extern "C" {
    struct Node {
        std::string raw_val;
        int file_idx;
        std::vector<std::string> row;
        bool is_numeric;
        bool is_desc;

        long get_num() const { return std::stol(raw_val); }
        
        // Переопределение оператора > для работы priority_queue в обоих направлениях
        bool operator>(const Node& other) const {
            if (is_numeric) {
                long a = get_num();
                long b = other.get_num();
                return is_desc ? (a < b) : (a > b); 
            } else {
                return is_desc ? (raw_val < other.raw_val) : (raw_val > other.raw_val);
            }
        }
    };

    // Добавлен параметр order_str ("asc" или "desc")
    void external_sort_cpp(const char* input_file, const char* output_file, const char* sort_key, const char* order_str, double* times) {
        clock_t total_start = clock();
        clock_t split_start = clock();

        bool is_desc = (strcmp(order_str, "desc") == 0);
        bool is_numeric = (strcmp(sort_key, "record_id") == 0 || strcmp(sort_key, "pet_id") == 0);

        std::ifstream infile(input_file);
        std::string header_line;
        std::getline(infile, header_line);

        std::stringstream ss(header_line);
        std::string token;
        int key_idx = 0;
        int curr_idx = 0;
        while (std::getline(ss, token, ',')) {
            if (token == sort_key) {
                key_idx = curr_idx;
                break;
            }
            curr_idx++;
        }

        std::vector<std::vector<std::string>> chunks;
        std::vector<std::string> temp_files;
        std::string line;
        int chunk_count = 0;
        int row_count = 0;
        int max_rows = 100000;

        while (std::getline(infile, line)) {
            std::stringstream ls(line);
            std::string cell;
            std::vector<std::string> row;
            while (std::getline(ls, cell, ',')) {
                row.push_back(cell);
            }
            chunks.push_back(row);
            row_count++;

            if (row_count >= max_rows) {
                std::sort(chunks.begin(), chunks.end(), [&](const std::vector<std::string>& a, const std::vector<std::string>& b) {
                    if (is_numeric) {
                        long val_a = std::stol(a[key_idx]);
                        long val_b = std::stol(b[key_idx]);
                        return is_desc ? (val_a > val_b) : (val_a < val_b);
                    } else {
                        return is_desc ? (a[key_idx] > b[key_idx]) : (a[key_idx] < b[key_idx]);
                    }
                });
                
                std::string temp_name = "temp_cpp_" + std::to_string(chunk_count) + ".csv";
                std::ofstream out(temp_name);
                for (const auto& r : chunks) {
                    for (size_t i = 0; i < r.size(); ++i) {
                        out << r[i];
                        if (i < r.size() - 1) out << ",";
                    }
                    out << "\n";
                }
                out.close();
                temp_files.push_back(temp_name);
                chunks.clear();
                chunk_count++;
                row_count = 0;
            }
        }
        
        if (!chunks.empty()) {
            std::sort(chunks.begin(), chunks.end(), [&](const std::vector<std::string>& a, const std::vector<std::string>& b) {
                if (is_numeric) {
                    long val_a = std::stol(a[key_idx]);
                    long val_b = std::stol(b[key_idx]);
                    return is_desc ? (val_a > val_b) : (val_a < val_b);
                } else {
                    return is_desc ? (a[key_idx] > b[key_idx]) : (a[key_idx] < b[key_idx]);
                }
            });
            std::string temp_name = "temp_cpp_" + std::to_string(chunk_count) + ".csv";
            std::ofstream out(temp_name);
            for (const auto& r : chunks) {
                for (size_t i = 0; i < r.size(); ++i) {
                    out << r[i];
                    if (i < r.size() - 1) out << ",";
                }
                out << "\n";
            }
            out.close();
            temp_files.push_back(temp_name);
        }
        infile.close();
        times[0] = (double)(clock() - split_start) / CLOCKS_PER_SEC;

        clock_t merge_start = clock();
        std::priority_queue<Node, std::vector<Node>, std::greater<Node>> pq;
        std::vector<std::ifstream*> files;
        
        for (size_t i = 0; i < temp_files.size(); ++i) {
            files.push_back(new std::ifstream(temp_files[i]));
            std::string l;
            if (std::getline(*files[i], l)) {
                std::stringstream ls(l);
                std::string cell;
                std::vector<std::string> row;
                while (std::getline(ls, cell, ',')) row.push_back(cell);
                Node n;
                n.raw_val = row[key_idx];
                n.file_idx = i;
                n.row = row;
                n.is_numeric = is_numeric;
                n.is_desc = is_desc;
                pq.push(n);
            }
        }

        std::ofstream outfile(output_file);
        outfile << header_line << "\n";
        while (!pq.empty()) {
            Node top = pq.top();
            pq.pop();
            for (size_t i = 0; i < top.row.size(); ++i) {
                outfile << top.row[i];
                if (i < top.row.size() - 1) outfile << ",";
            }
            outfile << "\n";

            std::string l;
            if (std::getline(*files[top.file_idx], l)) {
                std::stringstream ls(l);
                std::string cell;
                std::vector<std::string> row;
                while (std::getline(ls, cell, ',')) row.push_back(cell);
                Node n;
                n.raw_val = row[key_idx];
                n.file_idx = top.file_idx;
                n.row = row;
                n.is_numeric = is_numeric;
                n.is_desc = is_desc;
                pq.push(n);
            }
        }
        outfile.close();

        for (auto f : files) { f->close(); delete f; }
        for (const auto& tf : temp_files) { std::remove(tf.c_str()); }

        times[1] = (double)(clock() - merge_start) / CLOCKS_PER_SEC;
        times[2] = (double)(clock() - total_start) / CLOCKS_PER_SEC;
    }
}