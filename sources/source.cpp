// Copyright 2018 Your Name <your_email>

#include <header.hpp>

int main(int argc, char **argv) {
    if (argc) {
        std::string url = argv[0];
        uint64_t depth = std::stoi(argv[1]);
        uint64_t network_threads = std::stoi(argv[2]);
        uint64_t parser_threads = std::stoi(argv[3]);
        std::string output = argv[4];
        Crawler s(url, depth, network_threads, parser_threads, output);
        s.handler();
    }
    return 0;
}
